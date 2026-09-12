"""Bridge INKBIRD BLE readings into Home Assistant via MQTT discovery.

Run this on a machine with Bluetooth (e.g. a Raspberry Pi) next to the
sensor. It publishes readings to an MQTT broker using Home Assistant's MQTT
discovery format, so entities appear automatically under Settings ->
Devices & Services -> MQTT once the broker is reachable from both this
machine and Home Assistant.

Configuration is via environment variables:
    MQTT_HOST               broker hostname or IP (required)
    MQTT_PORT               broker port (default: 1883)
    MQTT_USERNAME           broker username (optional)
    MQTT_PASSWORD           broker password (optional)
    MQTT_BASE_TOPIC         state topic prefix (default: inkbird)
    MQTT_DISCOVERY_PREFIX   HA discovery prefix (default: homeassistant,
                             matches HA's default MQTT integration setting)
    MQTT_OFFLINE_TIMEOUT    seconds without a reading before a device is
                             marked unavailable in HA (default: 300)

Usage:
    pip install -r requirements.txt
    MQTT_HOST=192.168.1.10 python mqtt_bridge.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import sys
import time

import paho.mqtt.client as mqtt
from inkbird_ble import SensorUpdate
from inkbird_scan import INKBIRDScanner

MQTT_HOST = os.environ.get("MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "inkbird")
DISCOVERY_PREFIX = os.environ.get("MQTT_DISCOVERY_PREFIX", "homeassistant")
OFFLINE_TIMEOUT = float(os.environ.get("MQTT_OFFLINE_TIMEOUT", "300"))
STATUS_TOPIC = f"{BASE_TOPIC}/bridge/status"
_AVAILABILITY_CHECK_INTERVAL = 30

_announced: set[str] = set()
_last_seen: dict[str, float] = {}
_device_online: dict[str, bool] = {}


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _field_name(device_id: str | None, key: str) -> str:
    return f"{_slug(device_id)}_{key}" if device_id else key


def _availability_topic(address: str) -> str:
    return f"{BASE_TOPIC}/{_slug(address)}/availability"


def _mark_online(client: mqtt.Client, address: str) -> None:
    _last_seen[address] = time.monotonic()
    if not _device_online.get(address):
        client.publish(_availability_topic(address), "online", retain=True)
        _device_online[address] = True
        print(f"{address} is online")


async def _availability_checker(client: mqtt.Client) -> None:
    while True:
        await asyncio.sleep(_AVAILABILITY_CHECK_INTERVAL)
        now = time.monotonic()
        for address, last_seen in list(_last_seen.items()):
            if _device_online.get(address) and now - last_seen > OFFLINE_TIMEOUT:
                client.publish(_availability_topic(address), "offline", retain=True)
                _device_online[address] = False
                print(f"{address} marked offline (no reading for over {OFFLINE_TIMEOUT:.0f}s)")


def _publish_discovery(
    client: mqtt.Client,
    address: str,
    name: str | None,
    device_type: object,
    update: SensorUpdate,
    device_key,
) -> None:
    slug = _slug(address)
    field = _field_name(device_key.device_id, device_key.key)
    unique_id = f"{slug}_{field}"
    if unique_id in _announced:
        return

    description = update.entity_descriptions.get(device_key)
    sensor_value = update.entity_values[device_key]
    device_info = update.devices.get(device_key.device_id)

    payload = {
        "name": sensor_value.name or device_key.key.replace("_", " ").title(),
        "unique_id": unique_id,
        "object_id": unique_id,
        "state_topic": f"{BASE_TOPIC}/{slug}/state",
        "value_template": f"{{{{ value_json.{field} }}}}",
        "availability": [
            {"topic": STATUS_TOPIC},
            {"topic": _availability_topic(address)},
        ],
        "availability_mode": "all",
        "state_class": "measurement",
        "device": {
            "identifiers": [slug],
            "name": (device_info.name if device_info else None) or name or address,
            "manufacturer": (device_info.manufacturer if device_info else None) or "INKBIRD",
            "model": (device_info.model if device_info else None) or str(device_type),
        },
    }
    if description and description.device_class:
        payload["device_class"] = description.device_class.value
    if description and description.native_unit_of_measurement:
        payload["unit_of_measurement"] = description.native_unit_of_measurement.value
    if device_key.key == "signal_strength":
        payload["enabled_by_default"] = False

    config_topic = f"{DISCOVERY_PREFIX}/sensor/{unique_id}/config"
    result = client.publish(config_topic, json.dumps(payload), retain=True)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"Failed to publish discovery for {unique_id}: {mqtt.error_string(result.rc)}")
    else:
        print(f"Published discovery config for {unique_id} to {config_topic}")
    _announced.add(unique_id)


def _publish_state(client: mqtt.Client, address: str, update: SensorUpdate) -> None:
    slug = _slug(address)
    state = {
        _field_name(device_key.device_id, device_key.key): sensor_value.native_value
        for device_key, sensor_value in update.entity_values.items()
    }
    topic = f"{BASE_TOPIC}/{slug}/state"
    result = client.publish(topic, json.dumps(state, default=str), retain=True)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"Failed to publish to {topic}: {mqtt.error_string(result.rc)}")
    else:
        print(f"Published {topic}: {state}")


def _make_on_reading(client: mqtt.Client):
    def _on_reading(address: str, name: str | None, device_type: object, update: SensorUpdate) -> None:
        for device_key in update.entity_values:
            _publish_discovery(client, address, name, device_type, update, device_key)
        _mark_online(client, address)
        _publish_state(client, address, update)

    return _on_reading


def _on_connect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
    if reason_code == 0:
        print(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        client.publish(STATUS_TOPIC, "online", retain=True)
    else:
        print(f"MQTT connection failed: {reason_code}")


def _on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
    print(f"Disconnected from MQTT broker: {reason_code}")


async def main() -> None:
    if not MQTT_HOST:
        sys.exit("MQTT_HOST environment variable is required")

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.will_set(STATUS_TOPIC, "offline", retain=True)
    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.loop_start()

    checker_task = asyncio.create_task(_availability_checker(client))
    try:
        await INKBIRDScanner(_make_on_reading(client)).run()
    finally:
        checker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await checker_task
        client.publish(STATUS_TOPIC, "offline", retain=True)
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
