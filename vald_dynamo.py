#!/usr/bin/env python3
"""
VALD DynaMo — handheld dynamometer (e.g. DynaMo-08464).

Service:      569a1101-b87f-490c-92cb-11ed6a12fc81
Notify char:  569a2000-...   device → host
Write char:   569a2001-...   host → device (write-with-response)

The device sits behind a Laird Virtual Serial Port–style BLE-to-UART bridge,
so commands and responses are framed:

    [STX=0x02] [0x02] [CMD] [LEN] [payload...] [CHK] [ETX=0x03]
    CHK = (0xFF - sum(CMD, LEN, payload bytes)) & 0xFF

Startup handshake (each step followed by ~200 ms of settle, in order):

    1. 04 00                    get version       → response CMD 0x05
    2. 00 00                    get name          → CMD 0x01 + ASCII
    3. 1D 00                    get hardware info → CMD 0x1E
    4. 1A 00                    get BLE address   → CMD 0x1B
    5. 24 00                    get full config   → CMD 0x25
    6. 56 01 00                 init measurement  → CMD 0x09 ACK
    7. 10 00                    keepalive
    8. 50 05 F4 01 01 E1 00     configure 500 Hz  → CMD 0x51 ACK
    9. 53 00                    start streaming   → CMD 0x09 ACK

After step 9 the device emits a 3-byte stream header [0x00, ?, ?] (ignored)
followed by 2-byte sample packets:

    [0x04] [delta_int8]     signed 8-bit delta — accumulate to absolute
    force_N = accumulator * 0.1

Periodic 0x52 status frames may be appended to a sample (mixed-delivery
packet) — drop the tail and keep the leading sample.

Keepalive 10 00 must be sent every ~1 s during streaming, otherwise the
device stops emitting samples within ~2 s. To stop, send 04 00.

There is no hardware tare and no battery service. "Tare" here just resets
the accumulator so the next sample reads as zero.

Usage:
    python vald_dynamo.py                    # scan and pick
    python vald_dynamo.py --name DynaMo
    python vald_dynamo.py --addr AA:BB:CC:...
    python vald_dynamo.py --duration 30
    python vald_dynamo.py --csv out.csv
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

SERVICE_UUID = "569a1101-b87f-490c-92cb-11ed6a12fc81"
NOTIFY_CHAR  = "569a2000-b87f-490c-92cb-11ed6a12fc81"
WRITE_CHAR   = "569a2001-b87f-490c-92cb-11ed6a12fc81"

CMD_GET_VERSION  = bytes([0x04, 0x00])
CMD_GET_NAME     = bytes([0x00, 0x00])
CMD_GET_HW_INFO  = bytes([0x1d, 0x00])
CMD_GET_BLE_ADDR = bytes([0x1a, 0x00])
CMD_GET_CONFIG   = bytes([0x24, 0x00])
CMD_INIT_MEASURE = bytes([0x56, 0x01, 0x00])
CMD_CONFIGURE    = bytes([0x50, 0x05, 0xf4, 0x01, 0x01, 0xe1, 0x00])
CMD_START_STREAM = bytes([0x53, 0x00])
CMD_KEEPALIVE    = bytes([0x10, 0x00])
CMD_STOP         = bytes([0x04, 0x00])   # same wire bytes as get-version

FORCE_SCALE_N_PER_COUNT = 0.1
KEEPALIVE_INTERVAL_S    = 1.0


@dataclass
class Sample:
    timestamp_s: float
    force_n: float
    force_kg: float


def make_frame(payload: bytes) -> bytes:
    chk = (0xff - (sum(payload) & 0xff)) & 0xff
    return bytes([0x02, 0x02]) + payload + bytes([chk, 0x03])


KEEPALIVE_FRAME = make_frame(CMD_KEEPALIVE)


async def find_device(name: Optional[str], addr: Optional[str], scan_timeout: float) -> str:
    if addr:
        return addr
    print(f"Scanning for {scan_timeout:.0f}s...", file=sys.stderr)
    found = await BleakScanner.discover(timeout=scan_timeout, service_uuids=[SERVICE_UUID])
    if name:
        found = [d for d in found if d.name and name.lower() in d.name.lower()]
    if not found:
        all_devs = await BleakScanner.discover(timeout=scan_timeout)
        found = [d for d in all_devs if d.name and "dynamo" in d.name.lower()]
    if not found:
        sys.exit("No DynaMo found.")
    if len(found) == 1:
        print(f"Found {found[0].name} ({found[0].address})", file=sys.stderr)
        return found[0].address
    for i, d in enumerate(found):
        print(f"  [{i}] {d.address}  {d.name or '(no name)'}", file=sys.stderr)
    return found[int(input("Pick a device: ").strip())].address


async def send_framed(client: BleakClient, payload: bytes) -> None:
    await client.write_gatt_char(WRITE_CHAR, make_frame(payload), response=True)


async def handshake(client: BleakClient) -> None:
    """Run the documented startup sequence. Each step has a fixed settle.

    Notifications must already be subscribed before calling this — the device
    expects the CCCD to be active when the first response is generated.
    """
    steps = (
        CMD_GET_VERSION, CMD_GET_NAME, CMD_GET_HW_INFO,
        CMD_GET_BLE_ADDR, CMD_GET_CONFIG, CMD_INIT_MEASURE, CMD_KEEPALIVE,
    )
    for cmd in steps:
        await send_framed(client, cmd)
        await asyncio.sleep(0.2)


async def keepalive_loop(client: BleakClient) -> None:
    """Resend the framed keepalive every second until cancelled."""
    while True:
        try:
            await client.write_gatt_char(WRITE_CHAR, KEEPALIVE_FRAME, response=True)
        except Exception:
            # Mid-stream write races (e.g. during teardown) are not fatal.
            pass
        await asyncio.sleep(KEEPALIVE_INTERVAL_S)


async def stream(client: BleakClient, csv_writer, duration: Optional[float]) -> None:
    accumulator = 0
    session_start = time.monotonic()
    stop = asyncio.Event()

    def on_notify(_sender, data: bytearray) -> None:
        nonlocal accumulator
        b = bytes(data)
        if not b:
            return
        # Hot path: 2-byte streaming sample [0x04, delta_int8]. Mixed-delivery
        # packets may append a 0x52 status frame — keep the leading sample
        # and ignore the tail.
        if len(b) >= 2 and b[0] == 0x04:
            delta = b[1] - 256 if b[1] > 127 else b[1]
            accumulator += delta
            ts = time.monotonic() - session_start
            force_n = accumulator * FORCE_SCALE_N_PER_COUNT
            force_kg = force_n / 9.80665
            print(f"t={ts:7.3f}s  F={force_n:+8.2f} N  ({force_kg:+6.3f} kg)")
            if csv_writer:
                csv_writer.writerow([f"{ts:.6f}", f"{force_n:.4f}", f"{force_kg:.4f}"])
            return
        # 3-byte stream header [0x00, ?, ?] arrives once when streaming starts.
        if len(b) == 3 and b[0] == 0x00:
            return
        # Framed handshake responses and periodic 0x52 status frames begin
        # with 0x02 0x02 — we don't parse them.
        if len(b) >= 4 and b[0] == 0x02 and b[1] == 0x02:
            return

    await client.start_notify(NOTIFY_CHAR, on_notify)
    try:
        await handshake(client)
        await send_framed(client, CMD_CONFIGURE)
        await asyncio.sleep(0.1)
        await send_framed(client, CMD_START_STREAM)

        ka_task = asyncio.create_task(keepalive_loop(client))
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
            ka_task.cancel()
            try:
                await ka_task
            except asyncio.CancelledError:
                pass
            try:
                await send_framed(client, CMD_STOP)
            except Exception:
                pass
    finally:
        await client.stop_notify(NOTIFY_CHAR)


async def main(args: argparse.Namespace) -> None:
    address = await find_device(args.name, args.addr, args.scan_timeout)
    print(f"Connecting to {address}...", file=sys.stderr)

    async with BleakClient(address, timeout=20.0) as client:
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
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        pass
