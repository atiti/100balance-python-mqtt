import serial
import time
import struct
import binascii
import json
import argparse
import logging
from rich.logging import RichHandler
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()]
)
log = logging.getLogger("modbus")

MQTT_CLIENT_ID = "Linux_Modbus_Client"
MQTT_TOPIC_BASE = "100BalanceBMS/"
client = None
energy_in_total = 0.0
energy_out_total = 0.0

def connect_mqtt(host, username=None, password=None):
    global client
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, protocol=mqtt.MQTTv311)
    if username and password:
        client.username_pw_set(username, password)
    try:
        client.connect(host)
        log.info(f"\u2705 Connected to MQTT broker at {host}")
        publish_discovery()
    except Exception as e:
        log.error(f"\u274c MQTT connection failed: {e}")

def publish_discovery():
    discovery("BatteryVoltage", "Battery Voltage", "V", "voltage", "measurement")
    discovery("Current", "Current", "A", "current", "measurement")
    discovery("SOC", "State of Charge", "%", "battery", "measurement")
    discovery("RemainingCapacity", "Remaining Capacity", "Ah")
    discovery("TimeToGo", "Time to Go", "s")
    discovery("MosTemperature", "MOS Temperature", "\u00b0C", "temperature", "measurement")
    discovery("Power", "Power", "W", "power", "measurement")
    discovery("EnergyIn", "Energy In", "kWh", "energy", "total_increasing")
    discovery("EnergyOut", "Energy Out", "kWh", "energy", "total_increasing")
    for i in range(1, 17):
        discovery(f"Cell{i}Voltage", f"Cell {i} Voltage", "V", "voltage", "measurement")

def discovery(sensor_id, name, unit, device_class=None, state_class=None):
    topic = f"homeassistant/sensor/{MQTT_CLIENT_ID}/{sensor_id}/config"
    payload = {
        "name": name,
        "state_topic": f"{MQTT_TOPIC_BASE}{sensor_id}",
        "unique_id": f"{MQTT_CLIENT_ID}_{sensor_id}",
        "device": {
            "identifiers": [MQTT_CLIENT_ID],
            "manufacturer": "100BalanceBMS",
            "model": "Linux Modbus Bridge",
            "name": "100Balance BMS"
        },
        "unit_of_measurement": unit,
        "force_update": True
    }
    if device_class:
        payload["device_class"] = device_class
    if state_class:
        payload["state_class"] = state_class
    client.publish(topic, json.dumps(payload), retain=True)

def publish_mqtt(topic, value):
    try:
        client.publish(topic, str(value), retain=True)
    except Exception as e:
        log.error(f"MQTT publish error: {e}")

def disconnect_mqtt():
    try:
        if client:
            client.disconnect()
    except Exception as e:
        log.error(f"MQTT disconnect error: {e}")

def compute_crc(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc

def send_modbus_command(ser, command):
    try:
        crc = compute_crc(command)
        full_command = command + struct.pack('<H', crc)
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        ser.write(full_command)
        log.debug(f"Sent: {binascii.hexlify(full_command).decode()}")
        time.sleep(0.2)
        response = ser.read(512)
        if response:
            log.debug(f"Received: {binascii.hexlify(response).decode()}")
            return response
        return None
    except Exception as e:
        log.error(f"Send error: {e}")
        return None

def decode_response(response, mqtt_host, mqtt_user, mqtt_pass, interval_sec):
    global energy_in_total, energy_out_total
    try:
        if not response or len(response) < 5:
            return
        header_idx = response.find(b'\x51\x03')
        if header_idx == -1:
            log.warning("Modbus header not found.")
            return
        response = response[header_idx:]
        byte_count = response[2]
        if len(response) < 3 + byte_count + 2:
            return
        data = response[3:3 + byte_count]

        connect_mqtt(mqtt_host, mqtt_user, mqtt_pass)

        cell_voltages = {}
        voltage = current = power = soc = rem_capacity = timeToGo = mos_temp = None

        for i in range(0, byte_count, 2):
            reg = i // 2 + 1
            val = struct.unpack('>H', data[i:i+2])[0]

            if 1 <= reg <= 16 and 2500 <= val <= 4500:
                v = round(val / 1000, 3)
                cell_voltages[f"Cell{reg}"] = v
                publish_mqtt(f"{MQTT_TOPIC_BASE}Cell{reg}Voltage", v)

            elif 49 <= reg <= 52:
                temp = val - 40
                if -20 <= temp <= 80:
                    publish_mqtt(f"{MQTT_TOPIC_BASE}BatteryTemp{reg - 48}", temp)

            elif reg == 57:
                voltage = round(val * 0.1, 2)
                if 10 < voltage < 100:
                    publish_mqtt(f"{MQTT_TOPIC_BASE}BatteryVoltage", voltage)

            elif reg == 58:
                current = round((val - 30000) * 0.1, 2)
                if abs(current) < 500:
                    publish_mqtt(f"{MQTT_TOPIC_BASE}Current", current)

            elif reg == 59:
                soc = round(val / 10, 2)
                if 0 <= soc <= 100:
                    publish_mqtt(f"{MQTT_TOPIC_BASE}SOC", soc)

            elif reg == 76:
                rem_capacity = round(val * 0.1, 2)
                if 0 < rem_capacity < 1000:
                    publish_mqtt(f"{MQTT_TOPIC_BASE}RemainingCapacity", rem_capacity)

            elif reg == 89:
                power = val if current is None else (val * -1 if current < 0 else val)
                if abs(power) < 10000:
                    publish_mqtt(f"{MQTT_TOPIC_BASE}Power", power)
                    direction = "PowerOut" if current and current < 0 else "PowerIn"
                    publish_mqtt(f"{MQTT_TOPIC_BASE}{direction}", power)
                    energy = abs(power) / 1000 * (interval_sec / 3600)
                    if power > 0:
                        energy_in_total += energy
                        publish_mqtt(f"{MQTT_TOPIC_BASE}EnergyIn", round(energy_in_total, 5))
                    else:
                        energy_out_total += energy
                        publish_mqtt(f"{MQTT_TOPIC_BASE}EnergyOut", round(energy_out_total, 5))
                if rem_capacity and power:
                    timeToGo = (rem_capacity - 15) * 16 * 3.2 * 3600 / abs(power)
                    publish_mqtt(f"{MQTT_TOPIC_BASE}TimeToGo", int(timeToGo))
                    publish_mqtt(f"{MQTT_TOPIC_BASE}BatteryZeroTime", f"{int(timeToGo//3600)}hrs {int(timeToGo%60)}mins")

            elif reg == 91:
                temp = val - 40
                if 0 <= temp <= 150:
                    mos_temp = temp
                    publish_mqtt(f"{MQTT_TOPIC_BASE}MosTemperature", temp)

            elif reg == 100 and voltage and power:
                publish_mqtt(f"{MQTT_TOPIC_BASE}BatteryState", json.dumps({
                    "Dc": {"Power": power, "Voltage": voltage},
                    "Soc": soc,
                    "Capacity": rem_capacity,
                    "TimeToGo": timeToGo,
                    "Voltages": cell_voltages,
                    "Info": {"MaxChargeVoltage": 55.2, "MaxChargeCurrent": 80, "MaxDischarge": 100},
                    "System": {"MOSTemperature": mos_temp}
                }))

        summary = f"V={voltage}V, I={current}A, SOC={soc}%, P={power}W, RC={rem_capacity}Ah, T={mos_temp}°C"
        log.info(f"Summary: {summary}")

        disconnect_mqtt()

    except Exception as e:
        log.error(f"Decode error: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default="/dev/ttyUSB0", help="Serial port")
    parser.add_argument("--mqtt", default="localhost", help="MQTT broker")
    parser.add_argument("--user", help="MQTT username")
    parser.add_argument("--pass", dest="password", help="MQTT password")
    parser.add_argument("--interval", type=int, default=10, help="Polling interval (s)")
    args = parser.parse_args()

    log.info("\U0001f680 Starting BMS MQTT bridge")
    log.info(f"📱 Port: {args.port}, MQTT: {args.mqtt}, Interval: {args.interval}s")

    modbus_cmd = bytes.fromhex("81 03 00 00 00 7F")
    try:
        with serial.Serial(args.port, 9600, timeout=1) as ser:
            while True:
                response = send_modbus_command(ser, modbus_cmd)
                if response:
                    decode_response(response, args.mqtt, args.user, args.password, args.interval)
                time.sleep(args.interval)
    except Exception as e:
        log.error(f"Serial error: {e}")

if __name__ == "__main__":
    main()
