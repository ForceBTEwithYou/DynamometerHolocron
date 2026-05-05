#!/usr/bin/env python3
"""
Muscle Meter (MUSCLE_V2) — handheld BLE dynamometer.

Service:      0000ffe0-0000-1000-8000-00805f9b34fb  (FFE0)
Notify char:  0000ffe1-...                          (FFE1)  60-byte packets
Write char:   0000ffe2-...                          (FFE2)  command channel
Battery:      standard 0x180F / 0x2A19              (single byte 0..100)

Each 60-byte notification carries 20 × 3-byte triplets:

    [MSB] [LSB] [FLAG]

    force_kg  = (MSB << 8 | LSB) / 10.0     # always kg over the wire
    flag bit 0 (0x01)  display unit = kg
    flag bit 1 (0x02)  display unit = lb
    flag bit 4 (0x10)  compression mode (clear = tension)

The device transmits ~24.3 packets/s (×20 samples → ~485 Hz). It auto-streams
as soon as notifications are subscribed; no explicit start command is needed.
There is no documented tare command — the device auto-zeros at power-on.

Sign convention used here: tension is positive, compression is negative.

Usage:
    python muscle_meter.py                      # scan and pick
    python muscle_meter.py --name MUSCLE        # name substring filter
    python muscle_meter.py --addr AA:BB:CC:...  # connect by address
    python muscle_meter.py --duration 30        # 30 s capture, then exit
    python muscle_meter.py --csv out.csv        # log samples to CSV
    python muscle_meter.py --battery            # read battery, exit
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

SERVICE_UUID  = "0000ffe0-0000-1000-8000-00805f9b34fb"
NOTIFY_CHAR   = "0000ffe1-0000-1000-8000-00805f9b34fb"
WRITE_CHAR    = "0000ffe2-0000-1000-8000-00805f9b34fb"
BATTERY_CHAR  = "00002a19-0000-1000-8000-00805f9b34fb"

KG_TO_N = 9.80665
BYTES_PER_SAMPLE = 3
NOMINAL_PACKET_HZ = 24.3
NOMINAL_SAMPLE_HZ = NOMINAL_PACKET_HZ * 20  # ~485 Hz


@dataclass
class Sample:
    timestamp_s: float       # session-relative, evenly spaced from a sample counter
    force_n: float           # signed: tension positive, compression negative
    force_kg: float          # absolute kg (no sign)
    is_compression: bool


def decode_packet(data: bytes, sample_counter: int) -> list[Sample]:
    """Decode a 60-byte notification into up to 20 samples.

    `sample_counter` is the running index of the first sample in this packet.
    Per-sample timestamps are derived from it at the nominal rate, which is
    smoother than packet-arrival time (BLE batching makes arrivals bursty).
    """
    samples: list[Sample] = []
    n = len(data) // BYTES_PER_SAMPLE
    if n == 0:
        return samples
    period = 1.0 / NOMINAL_SAMPLE_HZ
    for i in range(n):
        off = i * BYTES_PER_SAMPLE
        msb, lsb, flag = data[off], data[off + 1], data[off + 2]
        kg = ((msb << 8) | lsb) / 10.0
        is_compression = bool(flag & 0x10)
        force_n = kg * KG_TO_N * (-1.0 if is_compression else 1.0)
        ts = (sample_counter + i) * period
        samples.append(Sample(ts, force_n, kg, is_compression))
    return samples


async def find_device(name: Optional[str], addr: Optional[str], scan_timeout: float) -> str:
    if addr:
        return addr
    print(f"Scanning for {scan_timeout:.0f}s...", file=sys.stderr)
    found = await BleakScanner.discover(timeout=scan_timeout, service_uuids=[SERVICE_UUID])
    if name:
        found = [d for d in found if d.name and name.lower() in d.name.lower()]
    # Fall back to a broad scan + name filter if the service-UUID scan returns nothing.
    if not found:
        all_devs = await BleakScanner.discover(timeout=scan_timeout)
        prefixes = ("muscle", "mm")
        found = [d for d in all_devs
                 if d.name and (name and name.lower() in d.name.lower()
                                or any(d.name.lower().startswith(p) for p in prefixes))]
    if not found:
        sys.exit("No Muscle Meter found.")
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
    sample_counter = 0
    stop = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        nonlocal sample_counter
        samples = decode_packet(bytes(data), sample_counter)
        if not samples:
            return
        sample_counter += len(samples)
        # Print one summary line per packet — not per-sample, which would be ~485 Hz.
        last = samples[-1]
        mode = "C" if last.is_compression else "T"
        print(f"t={last.timestamp_s:7.3f}s  F={last.force_n:+8.2f} N  ({last.force_kg:5.2f} kg) [{mode}]")
        if csv_writer:
            for s in samples:
                csv_writer.writerow([f"{s.timestamp_s:.6f}", f"{s.force_n:.4f}",
                                     f"{s.force_kg:.4f}", int(s.is_compression)])

    await client.start_notify(NOTIFY_CHAR, on_notify)
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
        await client.stop_notify(NOTIFY_CHAR)


async def main(args: argparse.Namespace) -> None:
    address = await find_device(args.name, args.addr, args.scan_timeout)
    print(f"Connecting to {address}...", file=sys.stderr)

    async with BleakClient(address, timeout=20.0) as client:
        if args.battery:
            pct = await read_battery(client)
            print(f"Battery: {pct}%" if pct >= 0 else "Battery: unavailable")
            return

        csv_file = open(args.csv, "w", newline="") if args.csv else None
        writer = None
        if csv_file is not None:
            writer = csv.writer(csv_file)
            writer.writerow(["timestamp_s", "force_n", "force_kg", "is_compression"])

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
