#!/usr/bin/env python3
"""
Tindeq Progressor — BLE force gauge.

Service:      7e4e1701-1ea6-40c9-9dcc-13d34ffead57
Notify char:  7e4e1702-...   data
Write char:   7e4e1703-...   control

Wire format on the data characteristic is TLV:

    [msg_type:u8] [payload_len:u8] [payload...]

    msg_type = 0x01  weight samples; payload is N × 8 bytes:
                       float32 LE (kg) + uint32 LE (device timestamp µs)
    msg_type = 0x00  command response (e.g. battery query):
                       payload bytes 0..3 are uint32 LE millivolts
    msg_type = 0x04  low-battery warning (no payload of interest)

Single-byte ASCII commands written to the control characteristic:

    'd' (0x64)  tare       — interrupts streaming; restart afterwards
    'e' (0x65)  start weight measurement
    'f' (0x66)  stop weight measurement
    'o' (0x6f)  get battery — response arrives as a 0x00 message

Effective rate ~80 Hz. The device timestamps each sample in microseconds
using its own clock, so we use those (drift-free, no BLE jitter). The µs
counter is a uint32 and wraps at ~71 minutes — handled here.

Tare sequence (per Tindeq docs): stop → small delay → tare → small delay →
start. Battery is approximated from voltage assuming a 3.0–4.2 V LiPo.

Usage:
    python tindeq.py                        # scan and pick
    python tindeq.py --name Progressor
    python tindeq.py --addr AA:BB:CC:...
    python tindeq.py --duration 30
    python tindeq.py --csv out.csv
    python tindeq.py --battery              # read battery, exit
    python tindeq.py --tare                 # zero, exit
"""

import argparse
import asyncio
import csv
import signal
import struct
import sys
from dataclasses import dataclass
from typing import Optional

from bleak import BleakClient, BleakScanner

SERVICE_UUID = "7e4e1701-1ea6-40c9-9dcc-13d34ffead57"
DATA_CHAR    = "7e4e1702-1ea6-40c9-9dcc-13d34ffead57"
CTRL_CHAR    = "7e4e1703-1ea6-40c9-9dcc-13d34ffead57"

CMD_TARE       = bytes([0x64])
CMD_START      = bytes([0x65])
CMD_STOP       = bytes([0x66])
CMD_GET_BATT   = bytes([0x6f])

MSG_CMD_RESPONSE = 0x00
MSG_WEIGHT       = 0x01
MSG_LOW_BATTERY  = 0x04

KG_TO_N = 9.80665
BYTES_PER_SAMPLE = 8     # float32 + uint32
UINT32_WRAP = 1 << 32    # device timestamp wraps at 2^32 µs (~71.6 min)


@dataclass
class Sample:
    timestamp_s: float
    force_n: float
    force_kg: float


class WeightDecoder:
    """Stateful decoder — tracks the device-µs baseline and uint32 wrap."""

    def __init__(self) -> None:
        self.start_us: Optional[int] = None
        self.last_us: int = 0

    def decode(self, data: bytes) -> list[Sample]:
        if len(data) < 2 or data[0] != MSG_WEIGHT:
            return []
        payload_len = data[1]
        n = payload_len // BYTES_PER_SAMPLE
        out: list[Sample] = []
        for i in range(n):
            off = 2 + i * BYTES_PER_SAMPLE
            if off + BYTES_PER_SAMPLE > len(data):
                break
            kg, dev_us = struct.unpack_from("<fI", data, off)
            # Wrap-around: large backward jump means uint32 rolled over.
            if dev_us < self.last_us and (self.last_us - dev_us) > 2_000_000_000:
                dev_us += UINT32_WRAP
            self.last_us = dev_us
            if self.start_us is None:
                self.start_us = dev_us
            ts_s = (dev_us - self.start_us) / 1_000_000.0
            out.append(Sample(ts_s, kg * KG_TO_N, kg))
        return out


def battery_pct_from_mv(mv: int) -> int:
    pct = ((mv - 3000) / 1200.0) * 100.0   # 3.0V = 0%, 4.2V = 100% (LiPo approx)
    return int(round(max(0.0, min(100.0, pct))))


async def find_device(name: Optional[str], addr: Optional[str], scan_timeout: float) -> str:
    if addr:
        return addr
    print(f"Scanning for {scan_timeout:.0f}s...", file=sys.stderr)
    found = await BleakScanner.discover(timeout=scan_timeout, service_uuids=[SERVICE_UUID])
    if name:
        found = [d for d in found if d.name and name.lower() in d.name.lower()]
    if not found:
        all_devs = await BleakScanner.discover(timeout=scan_timeout)
        found = [d for d in all_devs if d.name and "progressor" in d.name.lower()]
    if not found:
        sys.exit("No Tindeq Progressor found.")
    if len(found) == 1:
        print(f"Found {found[0].name} ({found[0].address})", file=sys.stderr)
        return found[0].address
    for i, d in enumerate(found):
        print(f"  [{i}] {d.address}  {d.name or '(no name)'}", file=sys.stderr)
    return found[int(input("Pick a device: ").strip())].address


async def read_battery(client: BleakClient, timeout_s: float = 3.0) -> int:
    """Send the get-battery command, capture the response, return percent."""
    mv: Optional[int] = None
    got = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        nonlocal mv
        b = bytes(data)
        if len(b) >= 6 and b[0] == MSG_CMD_RESPONSE:
            mv = struct.unpack_from("<I", b, 2)[0]
            got.set()

    await client.start_notify(DATA_CHAR, on_notify)
    try:
        await client.write_gatt_char(CTRL_CHAR, CMD_GET_BATT, response=True)
        try:
            await asyncio.wait_for(got.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            return -1
    finally:
        await client.stop_notify(DATA_CHAR)
    return battery_pct_from_mv(mv) if mv is not None else -1


async def do_tare(client: BleakClient) -> None:
    """Stop → small delay → tare → small delay → restart (then immediately stop)."""
    try:
        await client.write_gatt_char(CTRL_CHAR, CMD_STOP, response=True)
    except Exception:
        pass
    await asyncio.sleep(0.05)
    await client.write_gatt_char(CTRL_CHAR, CMD_TARE, response=True)
    await asyncio.sleep(0.05)
    # The TS reference restarts after tare so the device is ready to stream.
    # In a one-shot --tare invocation we stop again to leave the device idle.
    await client.write_gatt_char(CTRL_CHAR, CMD_START, response=True)
    await asyncio.sleep(0.05)
    try:
        await client.write_gatt_char(CTRL_CHAR, CMD_STOP, response=True)
    except Exception:
        pass


async def stream(client: BleakClient, csv_writer, duration: Optional[float]) -> None:
    decoder = WeightDecoder()
    stop = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        b = bytes(data)
        if not b:
            return
        if b[0] == MSG_LOW_BATTERY:
            print("Tindeq reports low battery.", file=sys.stderr)
            return
        if b[0] != MSG_WEIGHT:
            return
        for s in decoder.decode(b):
            print(f"t={s.timestamp_s:7.3f}s  F={s.force_n:8.2f} N  ({s.force_kg:6.3f} kg)")
            if csv_writer:
                csv_writer.writerow([f"{s.timestamp_s:.6f}",
                                     f"{s.force_n:.4f}", f"{s.force_kg:.4f}"])

    await client.start_notify(DATA_CHAR, on_notify)
    await client.write_gatt_char(CTRL_CHAR, CMD_START, response=True)
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
            await client.write_gatt_char(CTRL_CHAR, CMD_STOP, response=True)
        except Exception:
            pass
        await client.stop_notify(DATA_CHAR)


async def main(args: argparse.Namespace) -> None:
    address = await find_device(args.name, args.addr, args.scan_timeout)
    print(f"Connecting to {address}...", file=sys.stderr)

    async with BleakClient(address, timeout=20.0) as client:
        if args.battery:
            pct = await read_battery(client)
            print(f"Battery: {pct}%" if pct >= 0 else "Battery: unavailable")
            return
        if args.tare:
            await do_tare(client)
            print("Tare sent.")
            return

        csv_file = open(args.csv, "w", newline="") if args.csv else None
        writer = None
        if csv_file is not None:
            writer = csv.writer(csv_file)
            writer.writerow(["timestamp_s", "force_n", "force_kg"])

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
