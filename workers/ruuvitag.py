from mqtt import MqttMessage, MqttConfigMessage
from workers.base import BaseWorker

import logger

import asyncio
import os
import time


# Use Bleak backend for ruuvitag_sensor
os.environ['RUUVI_BLE_ADAPTER'] = 'bleak'

REQUIREMENTS = ["bleak", "ruuvitag_sensor"]

# Supports all attributes of Data Format 2, 3, 4 and 5 of the RuuviTag.
# See https://github.com/ruuvi/ruuvi-sensor-protocols for the sensor protocols.
# Available attributes:
# +-----------------------------+---+---+---+---+
# | Attribute / Data Format     | 2 | 3 | 4 | 5 |
# +-----------------------------+---+---+---+---+
# | acceleration                |   | X |   | X |
# | acceleration_x              |   | X |   | X |
# | acceleration_y              |   | X |   | X |
# | acceleration_z              |   | X |   | X |
# | battery                     |   | X |   | X |
# | data_format                 | X | X | X | X |
# | humidity                    | X | X | X | X |
# | identifier                  |   |   | X |   |
# | low_battery                 |   | X |   | X |
# | mac                         |   |   |   | X |
# | measurement_sequence_number |   |   |   | X |
# | movement_counter            |   |   |   | X |
# | pressure                    | X | X | X | X |
# | temperature                 | X | X | X | X |
# | tx_power                    |   |   |   | X |
# +-----------------------------+---+---+---+---+
ATTR_CONFIG = [
    # (attribute_name, device_class, unit_of_measurement)
    ("acceleration", "none", "mG"),
    ("acceleration_x", "none", "mG"),
    ("acceleration_y", "none", "mG"),
    ("acceleration_z", "none", "mG"),
    ("battery", "battery", "mV"),
    ("data_format", "none", ""),
    ("humidity", "humidity", "%"),
    ("identifier", "none", ""),
    ("mac", "none", ""),
    ("measurement_sequence_number", "none", ""),
    ("movement_counter", "none", ""),
    ("pressure", "pressure", "hPa"),
    ("temperature", "temperature", "°C"),
    ("tx_power", "none", "dBm"),
]
ATTR_LOW_BATTERY = "low_battery"
# "[Y]ou should plan to replace the battery when the voltage drops below 2.5 volts"
# Source: https://github.com/ruuvi/ruuvitag_fw/wiki/FAQ:-battery
LOW_BATTERY_VOLTAGE = 2500
_LOGGER = logger.get(__name__)


class RuuvitagWorker(BaseWorker):
    def _setup(self):
        _LOGGER.info("Adding %d %s devices", len(self.devices), repr(self))
        self.device_names = {mac: name for name, mac in self.devices.items()}
        if not hasattr(self, "no_data_timeout"):
            self.no_data_timeout = None

    def config(self, availability_topic):
        ret = []
        for name, mac in self.devices.items():
            ret.extend(self.config_device(name, mac))
        return ret

    def config_device(self, name, mac):
        ret = []
        device = {
            "identifiers": self.format_discovery_id(mac, name),
            "manufacturer": "Ruuvi",
            "model": "RuuviTag",
            "name": self.format_discovery_name(name),
        }

        for _, device_class, unit in ATTR_CONFIG:
            payload = {
                "unique_id": self.format_discovery_id(mac, name, device_class),
                "name": self.format_discovery_name(name, device_class),
                "state_topic": self.format_prefixed_topic(name, device_class),
                "device": device,
                "device_class": device_class,
                "unit_of_measurement": unit,
            }
            ret.append(
                MqttConfigMessage(
                    MqttConfigMessage.SENSOR,
                    self.format_discovery_topic(mac, name, device_class),
                    payload=payload,
                )
            )

        # Add low battery config
        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.BINARY_SENSOR,
                self.format_discovery_topic(mac, name, ATTR_LOW_BATTERY),
                payload={
                    "unique_id": self.format_discovery_id(mac, name, ATTR_LOW_BATTERY),
                    "name": self.format_discovery_name(name, ATTR_LOW_BATTERY),
                    "state_topic": self.format_prefixed_topic(name, ATTR_LOW_BATTERY),
                    "device": device,
                    "device_class": "battery",
                },
            )
        )

        return ret

    def run(self, mqtt):
        async def async_run():
            self.last_update = time.time()
            await asyncio.gather(
                update_sensors(),
                raise_on_interface_down(),
                raise_on_no_data_timeout()
            )

        async def update_sensors():
            """
            Query ruuvitag scans and publish them to MQTT.
            """
            try:
                from ruuvitag_sensor.ruuvi import RuuviTagSensor

                async for data in RuuviTagSensor.get_data_async(self.devices.values()):
                    mac, values = data
                    name = self.device_names[mac]
                    self.last_update = time.time()
                    _LOGGER.info("Updating %s '%s'", repr(self), name)
                    mqtt.publish(self.update_device_state(name, values))
            except Exception as e:
                self.log_unspecified_exception(_LOGGER, name, e)
                raise e
            else:
                raise RuntimeError("Ruuvitag updater has exited unexpectedly")

        async def raise_on_interface_down():
            """
            Bluetooth interface may go down while scan is running. Reason is
            unknown, happens with raspberry pi 3b+ with external bt dongle.
            This function raises error when the interface is down.
            """
            while True:
                await asyncio.sleep(60)
                if "UP RUNNING" not in (await run_command("hciconfig", "hci0")):
                    raise RuntimeError("Bluetooth interface has gone down")

        async def run_command(*args):
            """
            Run system command with given arguments. Returns stdout/stderr as
            a string.
            """
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            out, _ = await proc.communicate()
            return out.decode('utf8')

        async def raise_on_no_data_timeout():
            """
            Bluetooth stack may go in a state where interface is up and
            running, but no scans are received. Reason is unknown, happens
            with raspberry pi 3b+. This function raises error if no scans
            were received within configured timeout.
            """
            if self.no_data_timeout is None:
                return
            while (self.last_update + self.no_data_timeout) > time.time():
                await asyncio.sleep(1)
            raise RuntimeError("Timeout waiting for ruuvitag data")

        asyncio.run(async_run())


    def update_device_state(self, name, values):
        ret = []
        for attr, device_class, _ in ATTR_CONFIG:
            try:
                # Do not sent None values, as they may cause parsing errors at
                # receiving clients, this happens with Ruuvitag Pro which
                # doesn't have humidity or pressure sensors
                if values[attr] is None:
                    continue
                ret.append(
                    MqttMessage(
                        topic=self.format_topic(name, device_class),
                        payload=values[attr],
                    )
                )
            except KeyError:
                # The data format of this sensor doesn't have this attribute, so ignore it.
                pass

        # Low battery binary sensor
        #
        try:
            ret.append(
                MqttMessage(
                    topic=self.format_topic(name, ATTR_LOW_BATTERY),
                    payload=self.true_false_to_ha_on_off(
                        values["battery"] < LOW_BATTERY_VOLTAGE
                    ),
                )
            )
        except KeyError:
            # The data format of this sensor doesn't have the battery attribute, so ignore it.
            pass

        return ret
