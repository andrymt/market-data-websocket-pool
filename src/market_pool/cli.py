"""NDJSON collector; status and diagnostics go to stderr, trades to stdout or a file."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

from .adapters import ADAPTERS
from .model import symbol
from .pool import Pool, Settings


def positive(value: str) -> float:
    result = float(value)
    if not 0 < result < float("inf"):
        raise argparse.ArgumentTypeError("Must be a finite positive number")
    return result


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--exchanges", nargs="+", choices=ADAPTERS, default=list(ADAPTERS))
    p.add_argument("--symbols", nargs="+", type=symbol, default=["BTC-USD", "ETH-USD"])
    p.add_argument("--copies", type=int, choices=[1, 2], default=1)
    p.add_argument("--duration", type=positive, help="Stop after this many seconds")
    p.add_argument("--output", type=Path, help="Append records to this NDJSON file")
    p.add_argument("--queue-size", type=int, default=4096)
    p.add_argument("--read-timeout", type=positive, default=60)
    p.add_argument("--status-interval", type=positive, default=10)
    return p


async def run(args) -> None:
    settings = Settings(
        copies=args.copies, queue_size=args.queue_size, read_timeout=args.read_timeout
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows: asyncio.run still handles Ctrl+C.
            pass
    output = args.output.open("a", encoding="utf-8") if args.output else sys.stdout

    async def sink(event: dict) -> None:
        line = json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n"

        def write() -> None:
            output.write(line)
            output.flush()

        # One consumer awaits every write, preserving sink order without blocking socket readers.
        await asyncio.to_thread(write)

    pool = Pool(
        [ADAPTERS[x] for x in dict.fromkeys(args.exchanges)],
        tuple(dict.fromkeys(args.symbols)),
        sink,
        settings,
    )

    async def status_loop():
        while True:
            await asyncio.sleep(args.status_interval)
            print(json.dumps(pool.status(), allow_nan=False), file=sys.stderr, flush=True)

    async def timer():
        await asyncio.sleep(args.duration)
        stop.set()

    status = asyncio.create_task(status_loop())
    deadline = asyncio.create_task(timer()) if args.duration else None
    try:
        await pool.run(stop)
    finally:
        status.cancel()
        if deadline:
            deadline.cancel()
        await asyncio.gather(status, *([deadline] if deadline else []), return_exceptions=True)
        print(json.dumps(pool.status(), allow_nan=False), file=sys.stderr, flush=True)
        if args.output:
            output.close()


def main() -> None:
    p = parser()
    args = p.parse_args()
    if args.queue_size < 1:
        p.error("--queue-size must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
