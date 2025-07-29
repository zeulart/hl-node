import os
import time
from typing import Callable, Iterable, Optional
import json
from dataclasses import dataclass, asdict
from collections import defaultdict
import queue
from threading import Lock

# The number of seconds of inactivity before we declare the file "cold" and
# shut down the tailing thread.
INACTIVITY_TIMEOUT_SECONDS = 3 * 60  # 3 minutes

# How many bytes to request from the kernel on each read. Tune for workload.
CHUNK_SIZE = 1 << 16  # 64 KiB

# -----------------------------
# Periodic ingestion constants
# -----------------------------

# Interval (seconds) between periodic scans in ingest_periodic().
POLL_INTERVAL_SECONDS = 30
# Default maximum bytes to read per poll cycle in ingest_periodic().
MAX_BYTES_PER_CYCLE = 5 * CHUNK_SIZE  # 320 KiB


def _default_consumer(lines: Iterable[str]) -> None:
    """Default consumer that simply drains the iterable without processing.

    Suitable as a placeholder when no queue or processor is supplied.
    """
    for _ in lines:
        pass


# -----------------------------------
# Periodic (batch) ingestion only
# -----------------------------------


def ingest_periodic(
    file_path: str,
    process: Optional[Callable[[Iterable[str]], None]] = None,
    interval: int = POLL_INTERVAL_SECONDS,
    max_bytes: int = MAX_BYTES_PER_CYCLE,
    encoding: str = "utf-8",
    output_queue: Optional[queue.Queue] = None,
) -> None:
    """Check *file_path* every *interval* seconds and feed new lines to *process*.

    This variant is designed for batch/aggregated processing where near-realtime
    latency is not needed. It holds an open file descriptor, remembers the last
    read offset, and sleeps between iterations, so CPU usage remains near zero.

    If the file is truncated (e.g. log rotation), it automatically seeks back
    to the beginning.
    """

    if process is None:
        process = _default_consumer

    try:
        f = open(file_path, "rb")
    except FileNotFoundError:
        print(f"Error: File {file_path} not found. Exiting ingest process.")
        return

    print(
        f"Periodically tailing {file_path}: interval={interval}s, max_bytes={max_bytes}"
    )

    buf = bytearray()
    try:
        while True:
            # Detect truncation: if file size < current offset, seek to start.
            try:
                current_size = os.path.getsize(file_path)
            except FileNotFoundError:
                # File disappeared (rotated). Sleep and retry.
                time.sleep(interval)
                continue

            if f.tell() > current_size:
                f.seek(0)

            # Read up to *max_bytes* this cycle
            bytes_remaining = max_bytes
            new_data = False
            while bytes_remaining > 0:
                chunk = f.read(min(CHUNK_SIZE, bytes_remaining))
                if not chunk:
                    break
                new_data = True
                bytes_remaining -= len(chunk)
                buf.extend(chunk)

            if new_data:
                *lines, buf[:] = buf.split(b"\n")
                if lines:
                    decoded_lines = [ln.decode(encoding, errors="replace") for ln in lines]

                    if output_queue is not None:
                        for _line in decoded_lines:
                            output_queue.put(_line)

                    if process is not None:
                        try:
                            process(decoded_lines)
                        except Exception as proc_exc:
                            print(f"Error in log-line processor: {proc_exc}")

            # Sleep until next poll
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Periodic ingest interrupted by user.")
    except Exception as exc:
        print(f"An error occurred during periodic ingestion of {file_path}: {exc}")
    finally:
        try:
            f.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# DEPRECATED / Removed members:
# ---------------------------------------------------------------------------

# `ingest` (real-time tail) was present in earlier revisions. It has been
# intentionally removed in favour of `ingest_periodic` which sleeps between
# polls and is more CPU-efficient for use-cases where <1-minute latency is
# sufficient.

@dataclass
class Trade:
    timestamp: int  # ms since epoch
    coin: str
    price: float
    size: float
    is_buy: bool  # True if taker buy (aggressor buying)


def parse_trade(line: str) -> Optional[Trade]:
    """Parse a single log line (JSON) into a Trade object.

    Hyperliquid format: [wallet_address, trade_data_dict]
    Returns None if parsing fails.
    """
    try:
        data = json.loads(line)
        
        if not isinstance(data, list) or len(data) < 2:
            print(f"Invalid format: expected [wallet, trade_data] but got {type(data)}")
            return None
        
        trade_data = data[1]
        if not isinstance(trade_data, dict):
            print(f"Invalid trade_data: expected dict but got {type(trade_data)}")
            return None
        
        return Trade(
            timestamp=trade_data["time"],
            coin=trade_data["coin"],
            price=float(trade_data["px"]),
            size=float(trade_data["sz"]),
            is_buy=(trade_data["side"] == "B"),  # 'B' = buy, 'A' = sell
        )
        
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        print(f"Error parsing trade line: {exc}")
        print(f"Raw line: {line[:200]}...")  # Truncate very long lines
        return None

@dataclass
class BucketMetrics:
    open: float
    high: float
    low: float
    close: float
    volume: float  # total traded size
    volume_delta: float  # buy volume - sell volume
    tape_speed: float  # trades per second
    num_trades: int


class Aggregator:
    """Aggregates trades into time-bucketed metrics per coin.

    Metrics include OHLCV, volume delta (buy - sell), tape speed (trades/sec),
    and running cumulative volume delta.

    To extend: subclass and override process_batch to add custom metrics.
    """
    def __init__(self, bucket_size_sec: int = 30):
        self.bucket_size = bucket_size_sec
        self.metrics: dict[str, dict[int, BucketMetrics]] = defaultdict(lambda: defaultdict(lambda: None))
        self.cum_volume_delta: dict[str, float] = defaultdict(float)  # Continuous/cumulative per coin
        self._lock = Lock()

    def process_batch(self, trades: list[Trade]) -> dict[str, dict[int, BucketMetrics]]:
        """Aggregate a batch of trades into per-coin, per-bucket metrics.

        Returns newly computed buckets since last call.
        
        Buckets are aligned to multiples of bucket_size_sec from epoch (floor division).
        """
        with self._lock:
            new_buckets = defaultdict(lambda: defaultdict(lambda: None))
            temp_buckets = defaultdict(lambda: defaultdict(list))

            for trade in trades:
                bucket_start = (trade.timestamp // 1000 // self.bucket_size) * self.bucket_size
                temp_buckets[trade.coin][bucket_start].append(trade)

            for coin, coin_buckets in temp_buckets.items():
                for bucket, bucket_trades in coin_buckets.items():
                    if not bucket_trades:
                        continue

                    prices = [t.price for t in bucket_trades]
                    sizes = [t.size for t in bucket_trades]
                    is_buys = [t.is_buy for t in bucket_trades]

                    o = prices[0]
                    c = prices[-1]
                    h = max(prices)
                    l = min(prices)
                    vol = sum(sizes)
                    buy_vol = sum(s for s, buy in zip(sizes, is_buys) if buy)
                    sell_vol = vol - buy_vol
                    delta = buy_vol - sell_vol
                    num = len(bucket_trades)
                    speed = num / self.bucket_size

                    self.cum_volume_delta[coin] += delta

                    metrics = BucketMetrics(o, h, l, c, vol, delta, speed, num)
                    self.metrics[coin][bucket] = metrics
                    new_buckets[coin][bucket] = metrics

            return dict(new_buckets)  # Return plain dict for serialization if needed

def aggregate_process(
    lines: Iterable[str],
    aggregator: Aggregator,
    output_queue: Optional[queue.Queue] = None,
) -> None:
    """Process callback: parse lines → Trades → aggregate → push new metrics to queue."""
    trades: list[Trade] = []
    for ln in lines:
        t = parse_trade(ln)
        if t is not None:
            trades.append(t)
    new_metrics = aggregator.process_batch(trades)

    # The output dict has structure:
    # {"coin": {bucket_start_sec: BucketMetrics, ...}, ...}
    # Use asdict(metrics) for JSON serialization if needed.
    if output_queue is not None:
        for coin, buckets in new_metrics.items():
            for bucket_start, metrics in buckets.items():
                output_queue.put(
                    {
                        "coin": coin,
                        "bucket_start": bucket_start,
                        "metrics": asdict(metrics),
                        "cum_volume_delta": aggregator.cum_volume_delta[coin],
                    }
                )

# Example usage:
# aggregator = Aggregator(bucket_size_sec=30)
# q = queue.Queue()
# ingest_periodic("logs/trades.log", process=lambda lines: aggregate_process(lines, aggregator, q))