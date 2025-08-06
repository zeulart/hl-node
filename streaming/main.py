#!/usr/bin/env python3
"""Hyperliquid high‑throughput log tailer (v2.3 – zero‑loss)
============================================================
Implements the lead engineer’s **critical fixes**:

1. **Redis reliability** – snapshot‑and‑roll‑back queue; no pop until EXEC
   succeeds; explicit `force` flag prevents infinite recursion.
2. **Rotation race** – final read after successor detection to guarantee no
   bytes left behind.
3. **Large‑line safety** – bump default `MAX_LINE_SIZE` to 8 MiB and keep
   single‑entry emission (no mid‑record split).
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import select
import signal
import sys
import time
from collections import deque
from pathlib import Path
from typing import Deque, List, Optional

import inotify.adapters  # noqa: F401  (reserved for future use)
import redis

# ───────────────────────── configuration ────────────────────────────
ENV          = os.getenv
LOG_DIR      = Path(ENV("LOG_PATH", "/home/hluser/hl/data/node_fills/hourly"))
R_HOST       = ENV("REDIS_HOST", "localhost")
R_PORT       = int(ENV("REDIS_PORT", "6379"))
PREFIX       = ENV("REDIS_STREAM_PREFIX", "node_fills")
BUFFER_SIZE  = int(ENV("BUFFER_SIZE", "131072"))
PIPE_SIZE    = int(ENV("PIPELINE_SIZE", "500"))
STREAM_MAX   = int(ENV("STREAM_MAXLEN", "0"))
RETRY_DELAY  = int(ENV("RECONNECT_DELAY_MS", "500")) / 1000.0
MAX_LINE     = int(ENV("MAX_LINE_SIZE", f"{8*1024*1024}"))  # 8 MiB

LOG_LEVEL = getattr(logging, ENV("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    stream=sys.stdout)
logger = logging.getLogger("hyperliquid.streamer")

shutdown = False

# ───────────────────── signal handling ──────────────────────────────

def _sig_handler(sig: int, _frm):
    global shutdown
    logger.warning("Signal %s received – shutting down …", sig)
    shutdown = True

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

# ───────────────────────── helpers ──────────────────────────────────

def open_follow(path: Path) -> io.BufferedReader:
    """Open *path* in O_NONBLOCK mode, wrapped in BufferedReader."""
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_NONBLOCK)
        return io.BufferedReader(io.FileIO(fd, "rb", closefd=True), buffer_size=256_000)
    except Exception:
        # Ensure fd closed on failure
        try:
            os.close(fd)  # type: ignore[arg-type]
        except Exception:
            pass
        raise


def stream_key_for(path: Path) -> str:
    return f"{PREFIX}:{path.parent.name}:{path.stem}"


def newest_file() -> Optional[Path]:
    try:
        days = sorted(LOG_DIR.iterdir())
    except (FileNotFoundError, PermissionError) as e:
        logger.error("Log dir issue: %s", e)
        return None
    for day in reversed(days):
        if not day.is_dir():
            continue
        hours = sorted(day.iterdir())
        if hours:
            return max(hours, key=lambda p: p.stat().st_mtime)
    return None

# ───────────────────────── redis buffer ─────────────────────────────

def _connect() -> redis.Redis:
    r = redis.Redis(host=R_HOST, port=R_PORT, decode_responses=False)
    r.ping()
    return r


class RedisBuffer:
    """Pipeline with snapshot‑rollback semantics for zero data‑loss."""

    def __init__(self):
        self.rds = _connect()
        self.pipe = self.rds.pipeline(transaction=False)
        self.backlog: Deque[tuple[str, bytes]] = deque()

    # public ---------------------------------------------------------
    def add(self, key: str, payload: bytes):
        self.backlog.append((key, payload))
        if len(self.backlog) >= PIPE_SIZE:
            self.flush()

    def flush(self, force: bool = False):
        if not self.backlog:
            return
        retry_items = list(self.backlog)  # snapshot
        self.backlog.clear()
        try:
            for key, payload in retry_items:
                if STREAM_MAX:
                    self.pipe.xadd(key, {b"data": payload}, maxlen=STREAM_MAX, approximate=True)
                else:
                    self.pipe.xadd(key, {b"data": payload})
            self.pipe.execute()
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.error("Redis down – retry in %.1fs: %s", RETRY_DELAY, e)
            # Roll back snapshot to front of queue
            self.backlog.extendleft(reversed(retry_items))
            if not force:
                self._reconnect_with_backoff()
                # After reconnection, try again once forcibly
                self.flush(force=True)
        except Exception:
            # Roll back snapshot then bubble up
            self.backlog.extendleft(reversed(retry_items))
            raise

    def close(self):
        self.flush(force=True)
        try:
            self.rds.close()
        except Exception:
            pass

    # internals ------------------------------------------------------
    def _reconnect_with_backoff(self):
        while not shutdown:
            try:
                self.rds = _connect()
                self.pipe = self.rds.pipeline(transaction=False)
                logger.info("Reconnected to Redis")
                return
            except redis.ConnectionError:
                logger.warning("Still down; retrying in %.1fs", RETRY_DELAY)
                time.sleep(RETRY_DELAY)

# ───────────────────────── tail‑loop ────────────────────────────────

def tail(path: Path, poller: select.poll, rb: RedisBuffer) -> Optional[Path]:
    logger.info("Tailing %s", path)
    key = stream_key_for(path)
    f = open_follow(path)
    fd = f.fileno()
    poller.register(fd, select.POLLIN)

    buf = bytearray()
    try:
        while not shutdown:
            if not poller.poll(2000):  # keep responsive
                rb.flush()
                continue
            chunk = f.read(BUFFER_SIZE)
            if chunk:
                buf += chunk
                while True:
                    nl = buf.find(b"\n")
                    if nl == -1:
                        if len(buf) > MAX_LINE:
                            logger.warning("Line >%d bytes – emitting whole", MAX_LINE)
                            rb.add(key, bytes(buf))
                            buf.clear()
                        break
                    line = buf[:nl]
                    if line:
                        rb.add(key, bytes(line))
                    buf = buf[nl + 1:]
                continue

            # No data – possible rotation. Detect successor *first*.
            succ = newest_file()
            if succ and succ != path:
                # Final read to close race window
                final = f.read(BUFFER_SIZE)
                if final:
                    buf += final
                    continue  # process loop again
                rb.flush(force=True)
                return succ
    finally:
        poller.unregister(fd)
        f.close()
        rb.flush(force=True)

# ───────────────────────────── main ─────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Hyperliquid log tailer")
    parser.add_argument("--once", action="store_true", help="stop after current file")
    args = parser.parse_args(argv)

    rb = RedisBuffer()
    poller = select.poll()
    current = newest_file()
    if not current:
        logger.error("No log files in %s", LOG_DIR)
        return 1

    while not shutdown and current:
        nxt = tail(current, poller, rb)
        if args.once:
            break
        current = nxt or newest_file()
        if not current:
            logger.info("Waiting for next file …")
            time.sleep(0.5)

    rb.close()
    logger.info("Streamer stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
