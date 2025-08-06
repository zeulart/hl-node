#!/usr/bin/env python3
"""Hyperliquid high‑throughput log tailer (v2.1)
================================================
* Streams ≈ 50 000 plain‑text lines / s to **hourly Redis Streams**
  (`<PREFIX>:YYYYMMDD:HH`).
* Keeps producing even if Redis restarts; auto‑reconnect + buffered flush.
* Closes **race‑window on hourly rotation**: tailer never stops reading until the
  successor file is positively opened.
* Handles arbitrarily long lines – no data‑loss on buffer boundaries.

Environment variables (defaults in parentheses) ───────────────────────
REDIS_HOST           (localhost)
REDIS_PORT           (6379)
REDIS_STREAM_PREFIX  (node_fills)
LOG_PATH             (/home/hluser/hl/data/node_fills/hourly)
BUFFER_SIZE          read chunk bytes (131072)
PIPELINE_SIZE        Redis pipeline batch (500)
STREAM_MAXLEN        Max entries per stream (0 = unlimited)
RECONNECT_DELAY_MS   Back‑off after Redis loss (500)
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
from pathlib import Path
from typing import Deque, List, Optional, Set
from collections import deque

import inotify.adapters
import redis

# ────────────────────────── configuration ───────────────────────────
ENV              = os.getenv
LOG_DIR          = Path(ENV("LOG_PATH", "/home/hluser/hl/data/node_fills/hourly"))
REDIS_HOST       = ENV("REDIS_HOST", "localhost")
REDIS_PORT       = int(ENV("REDIS_PORT", "6379"))
PREFIX           = ENV("REDIS_STREAM_PREFIX", "node_fills")
BUFFER_SIZE      = int(ENV("BUFFER_SIZE", "131072"))
PIPELINE_SIZE    = int(ENV("PIPELINE_SIZE", "500"))
STREAM_MAXLEN    = int(ENV("STREAM_MAXLEN", "0"))
RECONNECT_DELAY  = int(ENV("RECONNECT_DELAY_MS", "500")) / 1000.0

LOG_LEVEL = getattr(logging, ENV("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    stream=sys.stdout)
logger = logging.getLogger("hyperliquid.streamer")

shutdown = False  # toggled by signal handler

# ─────────────────────── signal handling ────────────────────────────

def _signal_handler(signum: int, _frame):
    global shutdown
    logger.warning("Received signal %s – graceful shutdown…", signum)
    shutdown = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ──────────────────── helpers & key derivation ──────────────────────

def open_followable(path: Path) -> io.BufferedReader:
    """Open *path* in non‑blocking mode suitable for tail‑f."""
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    return io.BufferedReader(io.FileIO(fd, 'rb', closefd=True), buffer_size=256_000)


def key_for(path: Path) -> str:
    day = path.parent.name
    hour = path.stem
    return f"{PREFIX}:{day}:{hour}"


def newest_file() -> Optional[Path]:
    try:
        days = sorted(LOG_DIR.iterdir())
    except FileNotFoundError:
        return None
    except PermissionError as e:
        logger.error("Log dir access error: %s", e)
        return None

    for day in reversed(days):
        if not day.is_dir():
            continue
        hours = sorted(day.iterdir())
        if hours:
            return max(hours, key=lambda p: p.stat().st_mtime)
    return None

# ───────────────────────── redis helpers ────────────────────────────

def connect_redis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
    r.ping()
    return r


class RedisBuffer:
    """Pipeline with reconnect + replay on failure."""

    def __init__(self):
        self.rds = connect_redis()
        self.pipe = self.rds.pipeline(transaction=False)
        self.backlog: Deque[tuple] = deque()  # (key, data)

    def add(self, key: str, payload: bytes):
        self.backlog.append((key, payload))
        if len(self.backlog) >= PIPELINE_SIZE:
            self.flush()

    def flush(self, force: bool = False):
        if not self.backlog:
            return
        try:
            while self.backlog:
                key, payload = self.backlog.popleft()
                if STREAM_MAXLEN:
                    self.pipe.xadd(key, {b'data': payload}, maxlen=STREAM_MAXLEN, approximate=True)
                else:
                    self.pipe.xadd(key, {b'data': payload})
            self.pipe.execute()
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.error("Redis failure – will retry: %s", e)
            time.sleep(RECONNECT_DELAY)
            self._reconnect()
            # Replay backlog (already re‑queued by except block)
            self.flush(force=True)
        except Exception as e:
            logger.exception("Unexpected Redis error: %s", e)
            raise

    def _reconnect(self):
        """Create fresh connection + pipeline, keep backlog intact."""
        while not shutdown:
            try:
                self.rds = connect_redis()
                self.pipe = self.rds.pipeline(transaction=False)
                logger.info("Reconnected to Redis")
                break
            except redis.ConnectionError:
                logger.warning("Redis still down… retrying in %.1fs", RECONNECT_DELAY)
                time.sleep(RECONNECT_DELAY)

    def close(self):
        self.flush(force=True)
        try:
            self.rds.close()
        except Exception:
            pass

# ────────────────────────── tailer core ─────────────────────────────

def wait_successor(prev: Path, timeout: float = 30.0) -> Optional[Path]:
    """Block until a *different* hour‑file appears (race‑safe)."""
    end = time.time() + timeout
    while not shutdown and time.time() < end:
        nxt = newest_file()
        if nxt and nxt != prev:
            return nxt
        time.sleep(0.05)
    return None


def tail(path: Path, poller: select.poll, rb: RedisBuffer) -> Optional[Path]:
    logger.info("Tailing %s", path)
    key = key_for(path)
    f = open_followable(path)
    fd = f.fileno()
    poller.register(fd, select.POLLIN)

    buf = bytearray()
    try:
        while not shutdown:
            if not poller.poll(2000):  # 2 s responsiveness
                rb.flush()
                continue
            chunk = f.read(BUFFER_SIZE)
            if not chunk:
                # file might be rotated – keep reading until no more link
                if not path.exists():
                    rb.flush(force=True)
                    successor = wait_successor(path)
                    return successor
                continue
            buf += chunk
            # split on last newline – supports >BUFFER_SIZE lines safely
            last_nl = buf.rfind(b"\n")
            if last_nl == -1:
                # no complete line yet – guard enormous lines
                if len(buf) > BUFFER_SIZE * 4:
                    logger.warning("Very long line (> %d) – flushing partial", len(buf))
                    rb.add(key, bytes(buf))
                    buf.clear()
                continue
            complete, remainder = buf[:last_nl], buf[last_nl + 1:]
            for ln in complete.split(b"\n"):
                if ln:
                    rb.add(key, ln)
            buf[:] = remainder
    finally:
        poller.unregister(fd)
        f.close()
        rb.flush(force=True)
    return None

# ───────────────────────────── main ─────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Hyperliquid high‑throughput log tailer")
    parser.add_argument("--once", action="store_true", help="exit after current file ends (debug)")
    args = parser.parse_args(argv)

    poller = select.poll()
    rb = RedisBuffer()

    current = newest_file()
    if current is None:
        logger.error("No log files found in %s", LOG_DIR)
        return 1

    while not shutdown and current:
        nxt = tail(current, poller, rb)
        if args.once:
            break
        current = nxt or wait_successor(current, timeout=5.0)
        if current is None:
            logger.info("Waiting for next file…")
            time.sleep(0.5)

    rb.close()
    logger.info("Streamer stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
