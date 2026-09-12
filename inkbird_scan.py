"""Shared BLE scanning glue for INKBIRD devices, built on inkbird-ble.

Both monitor.py (console output) and mqtt_bridge.py (Home Assistant MQTT
discovery) reuse this to avoid duplicating the advertisement/poll/notify
handling.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from habluetooth import BluetoothServiceInfoBleak
from inkbird_ble import INKBIRDBluetoothDeviceData, SensorUpdate

ReadingCallback = Callable[[str, "str | None", object, SensorUpdate], None]


def _build_service_info(
    device: BLEDevice, adv: AdvertisementData
) -> BluetoothServiceInfoBleak:
    # On macOS, bleak's CoreBluetooth backend returns `device.name` as a
    # PyObjC string subclass rather than a plain `str`, which habluetooth's
    # strict type check rejects.
    name = str(device.name) if device.name is not None else None
    return BluetoothServiceInfoBleak(
        name=name,
        address=str(device.address),
        rssi=adv.rssi,
        manufacturer_data=adv.manufacturer_data,
        service_data=adv.service_data,
        service_uuids=adv.service_uuids,
        source="local",
        device=device,
        advertisement=adv,
        connectable=True,
        time=time.monotonic(),
        tx_power=adv.tx_power or 0,
        raw=None,
    )


class INKBIRDScanner:
    """Scans for INKBIRD BLE devices and reports readings via a callback."""

    def __init__(self, on_reading: ReadingCallback) -> None:
        self._on_reading = on_reading
        self._parsers: dict[str, INKBIRDBluetoothDeviceData] = {}
        self._last_poll: dict[str, float | None] = {}
        self._notify_started: set[str] = set()
        self._announced: set[str] = set()
        self._background_tasks: set[asyncio.Task] = set()

    def _spawn(self, coro: Awaitable[None]) -> None:
        task = asyncio.get_event_loop().create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _start_notify(
        self,
        address: str,
        name: str | None,
        device_type,
        device: BLEDevice,
        service_info: BluetoothServiceInfoBleak,
    ) -> None:
        def _on_update(update: SensorUpdate) -> None:
            self._on_reading(address, name, device_type, update)

        parser = INKBIRDBluetoothDeviceData(
            device_type,
            update_callback=_on_update,
            device_data_changed_callback=lambda _: None,
        )
        self._parsers[address] = parser
        try:
            await parser.async_start(service_info, device)
        except Exception as err:  # noqa: BLE001
            print(f"Error starting notifications for {name or address} ({address}): {err}")

    async def _poll(
        self,
        address: str,
        name: str | None,
        device_type,
        parser: INKBIRDBluetoothDeviceData,
        device: BLEDevice,
    ) -> None:
        try:
            update = await parser.async_poll(device)
            self._on_reading(address, name, device_type, update)
        except Exception as err:  # noqa: BLE001
            print(f"Error polling {name or address} ({address}): {err}")

    def _on_detection(self, device: BLEDevice, adv: AdvertisementData) -> None:
        address = device.address
        parser = self._parsers.get(address)
        if parser is None:
            parser = INKBIRDBluetoothDeviceData()
            self._parsers[address] = parser

        service_info = _build_service_info(device, adv)
        try:
            supported = parser.supported(service_info)
        except Exception:
            # inkbird-ble assumes an advertised name is present; nearby
            # non-INKBIRD devices (phones, earbuds, etc.) often broadcast
            # with no name at all, which would otherwise crash the scanner.
            return
        if not supported:
            return

        if address not in self._announced:
            self._announced.add(address)
            print(
                f"Discovered {parser.device_type} device: "
                f"{device.name or 'unknown'} ({address})"
            )

        if parser.uses_notify:
            if address in self._notify_started:
                return
            self._notify_started.add(address)
            self._spawn(
                self._start_notify(
                    address, device.name, parser.device_type, device, service_info
                )
            )
        elif parser.poll_needed(service_info, self._last_poll.get(address)):
            self._last_poll[address] = time.monotonic()
            self._spawn(self._poll(address, device.name, parser.device_type, parser, device))
        else:
            update = parser.update(service_info)
            self._on_reading(address, device.name, parser.device_type, update)

    async def run(self) -> None:
        print("Scanning for INKBIRD BLE devices. Press Ctrl+C to stop.\n")
        scanner = BleakScanner(detection_callback=self._on_detection)
        await scanner.start()
        try:
            while True:
                await asyncio.sleep(1)
        finally:
            await scanner.stop()
