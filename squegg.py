#!/usr/bin/env python3
"""
Squegg — squeeze-egg BLE grip strength sensor.

Service:      0000ffb0-0000-1000-8000-00805f9b34fb  (FFB0)
Notify char:  0000ffb2-...                          (FFB2)  14-byte ASCII packets
Write char:   0000ffb1-...                          (FFB1)  command channel

Packet format (14 bytes, ASCII, null-separated):

    byte 0       always '1' (ignored)
    byte 1       battery digit char  → percent = 10 * (digit + 1)
    byte 2       squeeze flag ('0' = idle, '1' = squeezing)
    bytes 3..    strength as ASCII float (kg), terminated by 0x00
    bytes ..end  squeeze count as ASCII integer (cumulative since power-on)

Example: b"18014.250\\x001084" → battery 90%, idle, 14.25 kg, count 1084.

The native rate is event-driven — roughly 5 Hz during an active squeeze and
sporadic (~1 Hz) at idle. Battery percent is embedded in every packet, so
--battery just reads the next notification rather than issuing a command.
There is no documented tare command; the device auto-zeros at power-on.

Note: an older Python library splits the 14-byte buffer the wrong way and
ends up parsing strength + count as a single garbage float. This decoder
splits on the 0x00 separator first, which is what the device intends.

Usage:
    python squegg.py                        # scan and pick
    python squegg.py --name Squegg
    python squegg.py --addr AA:BB:CC:...
    python squegg.py --duration 30
    python squegg.py --csv out.csv
    python squegg.py --battery              # read battery from next packet, exit
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

SERVICE_UUID  = "0000ffb0-0000-1000-8000-00805f9b34fb"
NOTIFY_CHAR   = "0000ffb2-0000-1000-8000-00805f9b34fb"
WRITE_CHAR    = "0000ffb1-0000-1000-8000-00805f9b34fb"

KG_TO_N = 9.80665


@dataclass
class Sample:
    timestamp_s: float
    force_n: float
    force_kg: float
    is_squeezing: bool
    battery_pct: int
    squeeze_count: int


def decode_packet(data: bytes, ts_s: float) -> Optional[Sample]:
    if len(data) < 4:
        return None
    try:
        battery_digit = data[1] - 0x30
        battery_pct = min(100, 10 * (battery_digit + 1))
        is_squeezing = data[2] == 0x31  # '1'

        payload = data[3:]
        parts = payload.split(b"\x00")
        strength_str = parts[0].decode("ascii", errors="ignore").strip()
        count_str = b"".join(parts[1:]).decode("ascii", errors="ignore").strip()
        kg = float(strength_str) if strength_str else 0.0
        count = int(count_str) if count_str else 0

        return Sample(ts_s, kg * KG_TO_N, kg, is_squeezing, battery_pct, count)
    except (ValueError, IndexError):
        return None


async def find_device(name: Optional[str], addr: Optional[str], scan_timeout: float) -> str:
    if addr:
        return addr
    print(f"Scanning for {scan_timeout:.0f}s...", file=sys.stderr)
    found = await BleakScanner.discover(timeout=scan_timeout, service_uuids=[SERVICE_UUID])
    if name:
        found = [d for d in found if d.name and name.lower() in d.name.lower()]
    if not found:
        all_devs = await BleakScanner.discover(timeout=scan_timeout)
        found = [d for d in all_devs if d.name and "squegg" in d.name.lower()]
    if not found:
        sys.exit("No Squegg found.")
    if len(found) == 1:
        print(f"Found {found[0].name} ({found[0].address})", file=sys.stderr)
        return found[0].address
    for i, d in enumerate(found):
        print(f"  [{i}] {d.address}  {d.name or '(no name)'}", file=sys.stderr)
    return found[int(input("Pick a device: ").strip())].address


async def read_battery_from_next_packet(client: BleakClient, timeout_s: float = 5.0) -> int:
    """Subscribe long enough to capture one packet, extract the battery byte."""
    pct = -1
    got = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        nonlocal pct
        s = decode_packet(bytes(data), 0.0)
        if s is not None:
            pct = s.battery_pct
            got.set()

    await client.start_notify(NOTIFY_CHAR, on_notify)
    try:
        await asyncio.wait_for(got.wait(), timeout=timeout_s)
    except asyncio.TimeoutError:
        pass
    finally:
        await client.stop_notify(NOTIFY_CHAR)
    return pct


async def stream(client: BleakClient, csv_writer, duration: Optional[float]) -> None:
    session_start = time.monotonic()
    stop = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        ts = time.monotonic() - session_start
        s = decode_packet(bytes(data), ts)
        if s is None:
            return
        flag = "SQ" if s.is_squeezing else "  "
        print(f"t={s.timestamp_s:7.3f}s  F={s.force_n:7.2f} N  ({s.force_kg:5.2f} kg) "
              f"{flag}  batt={s.battery_pct:3d}%  count={s.squeeze_count}")
        if csv_writer:
            csv_writer.writerow([f"{s.timestamp_s:.6f}", f"{s.force_n:.4f}",
                                 f"{s.force_kg:.4f}", int(s.is_squeezing),
                                 s.battery_pct, s.squeeze_count])

    await client.start_notify(NOTIFY_CHAR, on_notify)
    print("Streaming. Squeeze the device. Press Ctrl+C to stop.", file=sys.stderr)

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
        await client.stop_notify(NOTIFY_CHAR)


async def main(args: argparse.Namespace) -> None:
    address = await find_device(args.name, args.addr, args.scan_timeout)
    print(f"Connecting to {address}...", file=sys.stderr)

    async with BleakClient(address, timeout=20.0) as client:
        if args.battery:
            pct = await read_battery_from_next_packet(client)
            print(f"Battery: {pct}%" if pct >= 0 else "Battery: unavailable")
            return

        csv_file = open(args.csv, "w", newline="") if args.csv else None
        writer = None
        if csv_file is not None:
            writer = csv.writer(csv_file)
            writer.writerow(["timestamp_s", "force_n", "force_kg",
                             "is_squeezing", "battery_pct", "squeeze_count"])

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
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        pass
