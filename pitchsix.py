#!/usr/bin/env python3
"""
PitchSix Force Board — BLE force plate / hangboard dynamometer.

Services and characteristics:

    ForceBoard service:  9a88d67f-8df2-4afe-9e0d-c2bbbe773dd0
        rx (notify):     9a88d682-...   force samples (multiple per packet)
        tare (write):    9a88d683-...   write 0x01 to zero
    Weight service:      467a8516-6e39-11eb-9439-0242ac130002
        tx (write):      467a8517-...   command channel
    Battery service:     0000180f-0000-1000-8000-00805f9b34fb
        battery level:   00002a19-...   single byte 0..100

Streaming packet format on rx:

    bytes 0..1     uint16 big-endian, sample count (N)
    bytes 2..      N × 3-byte big-endian samples

    raw_lbs = b[0]*32768 + b[1]*256 + b[2]
    force_N = raw_lbs * 4.44822

The wire unit is integer pounds — the signal is therefore quantized
(staircase). The device is tension-only per its API; loading in compression
produces unsigned-overflow samples (>>1100 lbs). Those are clamped to 0 N
and a one-time warning is printed.

Commands written to tx (single-byte opcode):

    0x04   start continuous streaming
    0x07   stop streaming

Tare is its own characteristic — write 0x01 to it (no stop/restart needed).

Effective rate: ~80 Hz on current firmware, ~40 Hz on older firmware.

Usage:
    python pitchsix.py                         # scan and pick
    python pitchsix.py --name "Force Board"    # name substring filter
    python pitchsix.py --addr AA:BB:CC:...
    python pitchsix.py --duration 30
    python pitchsix.py --csv out.csv
    python pitchsix.py --tare                  # zero, exit
    python pitchsix.py --battery               # read battery, exit
"""

import argparse
import asyncio
import csv
import signal
import sys
import time
from dataclasses import dataclass
from typing import Optional

from bleak import BleakClient, BleakScanner

FORCEBOARD_SVC = "9a88d67f-8df2-4afe-9e0d-c2bbbe773dd0"
RX_CHAR        = "9a88d682-8df2-4afe-9e0d-c2bbbe773dd0"
TARE_CHAR      = "9a88d683-8df2-4afe-9e0d-c2bbbe773dd0"
WEIGHT_SVC     = "467a8516-6e39-11eb-9439-0242ac130002"
TX_CHAR        = "467a8517-6e39-11eb-9439-0242ac130002"
BATTERY_CHAR   = "00002a19-0000-1000-8000-00805f9b34fb"

CMD_START = bytes([0x04])
CMD_STOP  = bytes([0x07])
CMD_TARE  = bytes([0x01])

LBS_TO_N   = 4.44822
LBS_TO_KG  = 0.45359237
BYTES_PER_SAMPLE = 3
MAX_VALID_LBS = 1100   # 450 kg model maxes around 992 lbs; >1100 = overflow


@dataclass
class Sample:
    timestamp_s: float
    force_n: float
    force_kg: float
    raw_lbs: int
    overflow: bool


def decode_packet(data: bytes, packet_arrival_s: float, est_interval_s: float) -> list[Sample]:
    """Decode one rx notification.

    The packet's last sample is timestamped at arrival; earlier samples step
    back by `est_interval_s`. Samples above `MAX_VALID_LBS` are clamped to 0
    and flagged as overflow.
    """
    if len(data) < 2:
        return []
    n = (data[0] << 8) | data[1]
    if n <= 0 or len(data) < 2 + n * BYTES_PER_SAMPLE:
        return []
    samples: list[Sample] = []
    for i in range(n):
        off = 2 + i * BYTES_PER_SAMPLE
        raw = data[off] * 32768 + data[off + 1] * 256 + data[off + 2]
        overflow = raw > MAX_VALID_LBS
        if overflow:
            force_n = 0.0
            force_kg = 0.0
            raw_lbs = 0
        else:
            force_n = raw * LBS_TO_N
            force_kg = raw * LBS_TO_KG
            raw_lbs = raw
        ts = packet_arrival_s - (n - 1 - i) * est_interval_s
        samples.append(Sample(ts, force_n, force_kg, raw_lbs, overflow))
    return samples


async def find_device(name: Optional[str], addr: Optional[str], scan_timeout: float) -> str:
    if addr:
        return addr
    print(f"Scanning for {scan_timeout:.0f}s...", file=sys.stderr)
    found = await BleakScanner.discover(timeout=scan_timeout, service_uuids=[FORCEBOARD_SVC])
    if name:
        found = [d for d in found if d.name and name.lower() in d.name.lower()]
    if not found:
        all_devs = await BleakScanner.discover(timeout=scan_timeout)
        found = [d for d in all_devs if d.name and "force board" in d.name.lower()]
    if not found:
        sys.exit("No Force Board found.")
    if len(found) == 1:
        print(f"Found {found[0].name} ({found[0].address})", file=sys.stderr)
        return found[0].address
    for i, d in enumerate(found):
        print(f"  [{i}] {d.address}  {d.name or '(no name)'}", file=sys.stderr)
    return found[int(input("Pick a device: ").strip())].address


async def read_battery(client: BleakClient) -> int:
    try:
        v = await client.read_gatt_char(BATTERY_CHAR)
        return int(v[0])
    except Exception:
        return -1


async def stream(client: BleakClient, csv_writer, duration: Optional[float]) -> None:
    session_start = time.monotonic()
    est_interval_s = 1.0 / 80.0   # initial guess, refined from real packet arrivals
    last_arrival_s: Optional[float] = None
    overflow_warned = False
    stop = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        nonlocal est_interval_s, last_arrival_s, overflow_warned
        arrival = time.monotonic() - session_start
        samples = decode_packet(bytes(data), arrival, est_interval_s)
        if not samples:
            return
        # Refine inter-sample interval estimate from packet-to-packet spacing.
        if last_arrival_s is not None:
            dt = arrival - last_arrival_s
            if dt > 0:
                est_interval_s = dt / max(len(samples), 1)
        last_arrival_s = arrival

        last = samples[-1]
        if last.overflow and not overflow_warned:
            print("WARNING: overflow detected — Force Board is tension-only; "
                  "clamping to 0 N. (Suppressing further warnings.)", file=sys.stderr)
            overflow_warned = True
        print(f"t={last.timestamp_s:7.3f}s  F={last.force_n:8.2f} N  "
              f"({last.force_kg:5.2f} kg, {last.raw_lbs:4d} lbs)")
        if csv_writer:
            for s in samples:
                csv_writer.writerow([f"{s.timestamp_s:.6f}", f"{s.force_n:.4f}",
                                     f"{s.force_kg:.4f}", s.raw_lbs, int(s.overflow)])

    await client.start_notify(RX_CHAR, on_notify)
    await client.write_gatt_char(TX_CHAR, CMD_START, response=False)
    print("Streaming. Press Ctrl+C to stop.", file=sys.stderr)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    try:
        if duration is not None:
            await asyncio.wait_for(stop.wait(), timeout=duration)
        else:
            await stop.wait()
    except asyncio.TimeoutError:
        pass
    finally:
        try:
            await client.write_gatt_char(TX_CHAR, CMD_STOP, response=False)
        except Exception:
            pass
        await client.stop_notify(RX_CHAR)


async def main(args: argparse.Namespace) -> None:
    address = await find_device(args.name, args.addr, args.scan_timeout)
    print(f"Connecting to {address}...", file=sys.stderr)

    async with BleakClient(address, timeout=20.0) as client:
        if args.battery:
            pct = await read_battery(client)
            print(f"Battery: {pct}%" if pct >= 0 else "Battery: unavailable")
            return
        if args.tare:
            await client.write_gatt_char(TARE_CHAR, CMD_TARE, response=False)
            print("Tare sent.")
            return

        csv_file = open(args.csv, "w", newline="") if args.csv else None
        writer = None
        if csv_file is not None:
            writer = csv.writer(csv_file)
            writer.writerow(["timestamp_s", "force_n", "force_kg", "raw_lbs", "overflow"])

        try:
            await stream(client, writer, args.duration)
        finally:
            if csv_file is not None:
                csv_file.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--name", help="Name substring to match while scanning")
    p.add_argument("--addr", help="Connect directly to this BLE address")
    p.add_argument("--csv", help="Write samples to this CSV file")
    p.add_argument("--duration", type=float, help="Stop after N seconds")
    p.add_argument("--scan-timeout", type=float, default=8.0)
    p.add_argument("--battery", action="store_true", help="Read battery, print, exit")
    p.add_argument("--tare", action="store_true", help="Send tare, exit")
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        pass
