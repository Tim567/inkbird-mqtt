"""Standalone INKBIRD BLE sensor monitor (console output).

Reuses the same parsing library (`inkbird-ble`) that Home Assistant's
`inkbird` integration is built on, but talks to Bluetooth directly via
`bleak` instead of requiring a full Home Assistant install.

Usage:
    pip install -r requirements.txt
    python monitor.py
"""

from __future__ import annotations

import asyncio
from datetime import datetime

from inkbird_ble import SensorUpdate
from inkbird_scan import INKBIRDScanner


def _print_reading(address: str, name: str | None, device_type: object, update: SensorUpdate) -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    label = name or address
    for device_key, sensor_value in update.entity_values.items():
        print(f"[{stamp}] {label} ({address}) {device_key.key} = {sensor_value.native_value}")


async def main() -> None:
    await INKBIRDScanner(_print_reading).run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
