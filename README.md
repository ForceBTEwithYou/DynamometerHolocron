# BTE Dynamometer CLIs

Self-contained Python scripts for talking to five BLE force/grip dynamometers.
Each script documents its device's protocol in the module docstring and
exposes a small `argparse` CLI to scan, connect, stream samples to the
console, and optionally log to CSV.

## Requirements

- Python ≥ 3.9
- A working BLE adapter (Linux/BlueZ, macOS, or Windows)
- `pip install -r requirements.txt`  (just `bleak`)

## Devices

| Script | Device | Service | Effective rate | Notes |
|---|---|---|---|---|
| [muscle_meter.py](muscle_meter.py) | Muscle Meter (MUSCLE_V2) | `FFE0` | ~485 Hz | 60-byte packets of 20 × 3-byte triplets; tension/compression flag |
| [pitchsix.py](pitchsix.py) | PitchSix Force Board | `9a88d67f-…` | ~80 Hz | Tension only; integer lbs over the wire |
| [squegg.py](squegg.py) | Squegg | `FFB0` | ~5 Hz | ASCII packets; battery embedded in payload |
| [tindeq.py](tindeq.py) | Tindeq Progressor | `7e4e1701-…` | ~80 Hz | TLV format; float32 kg + device µs timestamp |
| [vald_dynamo.py](vald_dynamo.py) | VALD DynaMo | `569a1101-…` | ~225 Hz | Framed handshake; delta-encoded samples; 1 Hz keepalive required |

## Usage

Each script has the same core flags:

```
python <device>.py                    # scan, pick a device, stream to console
python <device>.py --name MUSCLE      # connect to the first match by name substring
python <device>.py --addr AA:BB:CC:DD:EE:FF
python <device>.py --duration 60      # capture for 60 s then exit
python <device>.py --csv out.csv      # also write samples to CSV
```

Where supported:

```
python <device>.py --battery          # read battery, print, exit
python <device>.py --tare             # zero the device, exit
```

`--battery` works on muscle_meter, pitchsix, squegg, tindeq.
`--tare` works on pitchsix and tindeq (vald_dynamo has no hardware tare;
muscle_meter and squegg auto-zero on power-on).

Press `Ctrl+C` to stop streaming at any time.

## Protocol details

The module docstring at the top of each script contains the full protocol:
service / characteristic UUIDs, packet layout, command bytes, decoding
math, sampling rate, and quirks. Skim those for an overview, or read them
when porting to another platform.
