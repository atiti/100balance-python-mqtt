#!/usr/bin/env python3
import os
import time
import json
import struct
import binascii
import argparse
import logging
from typing import Optional, Dict, Any, Tuple

import serial
import paho.mqtt.client as mqtt

# -----------------------------
# Logging
# -----------------------------
log = logging.getLogger("100balance-bms")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)

# -----------------------------
# Defaults
# -----------------------------
MQTT_CLIENT_ID = "100BalanceBMS"
MQTT_TOPIC_BASE = "100BalanceBMS/"

# Persisted totals (optional; writes are rate-limited to reduce SD wear)
DEFAULT_STATE_PATH = "/var/lib/100balance-bms/state.json"

# Your pack: 24V 7S LiFePO4
DEFAULT_CELL_COUNT = 7

# Reserve (Ah) you never want to consume (optional)
DEFAULT_RESERVE_AH = 15.0

# -----------------------------
# Modbus helpers
# -----------------------------
def compute_crc(data: bytes) -> int:
    """Modbus RTU CRC16 (poly 0xA001). Returns int 0..65535."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if (crc & 1) else (crc >> 1)
    return crc & 0xFFFF

def validate_rtu_frame(frame: bytes) -> bool:
    """Frame includes CRC at end (2 bytes, little-endian)."""
    if len(frame) < 5:
        return False
    data = frame[:-2]
    recv_crc = frame[-2] | (frame[-1] << 8)
    calc_crc = compute_crc(data)
    return recv_crc == calc_crc

def find_frame(buf: bytes, addr: int, func: int) -> Optional[bytes]:
    """
    Find a Modbus RTU frame in a buffer:
    [addr][func][byte_count][data...][crc_lo][crc_hi]
    Returns the sliced frame if CRC passes, else None.
    """
    needle = bytes([addr, func])
    idx = buf.find(needle)
    if idx == -1:
        return None
    if len(buf) < idx + 3:
        return None

    byte_count = buf[idx + 2]
    frame_len = 3 + byte_count + 2
    if len(buf) < idx + frame_len:
        return None

    frame = buf[idx : idx + frame_len]
    if not validate_rtu_frame(frame):
        return None
    return frame

# -----------------------------
# MQTT / HA Discovery
# -----------------------------
def ha_device() -> Dict[str, Any]:
    return {
        "identifiers": [MQTT_CLIENT_ID],
        "manufacturer": "100BalanceBMS",
        "model": "BMS → MQTT Bridge",
        "name": "100Balance BMS",
    }

class MqttHa:
    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        # paho-mqtt v2 callback API (avoids DeprecationWarning)
        self.client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv311,
        )
        if username:
            self.client.username_pw_set(username, password)

    def connect(self) -> None:
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()
        log.info(f"✅ Connected to MQTT broker at {self.host}:{self.port}")

    def publish(self, topic: str, payload: Any, retain: bool = True) -> None:
        self.client.publish(topic, str(payload), retain=retain)

    def discovery_sensor(
        self,
        object_id: str,
        name: str,
        unit: Optional[str] = None,
        device_class: Optional[str] = None,
        state_class: Optional[str] = None,
        icon: Optional[str] = None,
    ) -> None:
        topic = f"homeassistant/sensor/{MQTT_CLIENT_ID}/{object_id}/config"
        payload: Dict[str, Any] = {
            "name": name,
            "state_topic": f"{MQTT_TOPIC_BASE}{object_id}",
            "unique_id": f"{MQTT_CLIENT_ID}_{object_id}",
            "device": ha_device(),
            "force_update": True,
        }
        if unit is not None:
            payload["unit_of_measurement"] = unit
        if device_class is not None:
            payload["device_class"] = device_class
        if state_class is not None:
            payload["state_class"] = state_class
        if icon is not None:
            payload["icon"] = icon

        self.client.publish(topic, json.dumps(payload), retain=True)

    def discovery_binary_sensor(
        self,
        object_id: str,
        name: str,
        device_class: Optional[str] = None,
        icon: Optional[str] = None,
    ) -> None:
        topic = f"homeassistant/binary_sensor/{MQTT_CLIENT_ID}/{object_id}/config"
        payload: Dict[str, Any] = {
            "name": name,
            "state_topic": f"{MQTT_TOPIC_BASE}{object_id}",
            "unique_id": f"{MQTT_CLIENT_ID}_{object_id}",
            "device": ha_device(),
            "payload_on": "ON",
            "payload_off": "OFF",
        }
        if device_class is not None:
            payload["device_class"] = device_class
        if icon is not None:
            payload["icon"] = icon
        self.client.publish(topic, json.dumps(payload), retain=True)

    def disconnect(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

def publish_discovery(ha: MqttHa, cell_count: int) -> None:
    ha.discovery_sensor("BatteryVoltage", "Battery Voltage", "V", "voltage", "measurement")
    ha.discovery_sensor("Current", "Battery Current", "A", "current", "measurement")
    ha.discovery_sensor("SOC", "State of Charge", "%", "battery", "measurement")
    ha.discovery_sensor("RemainingCapacity", "Remaining Capacity", "Ah", None, "measurement")
    ha.discovery_sensor("Power", "Battery Power (V*I)", "W", "power", "measurement")
    ha.discovery_sensor("PowerRawReg89", "Battery Power (raw reg89)", "W", "power", "measurement", icon="mdi:code-braces")
    ha.discovery_sensor("EnergyIn", "Energy In", "kWh", "energy", "total_increasing")
    ha.discovery_sensor("EnergyOut", "Energy Out", "kWh", "energy", "total_increasing")
    ha.discovery_sensor("TimeToGo", "Time to Go", "s", None, "measurement", icon="mdi:timer-sand")
    ha.discovery_sensor("BatteryZeroTime", "Battery Zero Time", None, None, None, icon="mdi:clock-outline")
    ha.discovery_sensor("MosTemperature", "MOS Temperature", "°C", "temperature", "measurement")

    # Extra temps (if present)
    for i in range(1, 5):
        ha.discovery_sensor(f"BatteryTemp{i}", f"Battery Temp {i}", "°C", "temperature", "measurement")

    # Directional helpers
    ha.discovery_sensor("PowerIn", "Charge Power", "W", "power", "measurement", icon="mdi:battery-arrow-up")
    ha.discovery_sensor("PowerOut", "Discharge Power", "W", "power", "measurement", icon="mdi:battery-arrow-down")
    ha.discovery_binary_sensor("Charging", "Charging", device_class="battery", icon="mdi:battery-plus")
    ha.discovery_binary_sensor("Discharging", "Discharging", device_class="battery", icon="mdi:battery-minus")

    # Cells (publish up to 16; HA doesn’t care; but we’ll mainly fill first cell_count)
    for i in range(1, 17):
        ha.discovery_sensor(f"Cell{i}Voltage", f"Cell {i} Voltage", "V", "voltage", "measurement")

# -----------------------------
# Energy totals persistence (rate-limited writes)
# -----------------------------
def load_state(path: str) -> Tuple[float, float]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            s = json.load(f)
        return float(s.get("energy_in_kwh", 0.0)), float(s.get("energy_out_kwh", 0.0))
    except Exception:
        return 0.0, 0.0

def save_state(path: str, energy_in_kwh: float, energy_out_kwh: float) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            {"energy_in_kwh": energy_in_kwh, "energy_out_kwh": energy_out_kwh, "ts": int(time.time())},
            f,
        )
    os.replace(tmp, path)

# -----------------------------
# Decode mapping (your existing assumptions + safer behavior)
# -----------------------------
def decode_registers_from_frame(frame: bytes) -> Dict[int, int]:
    """
    Returns dict: reg_index (1-based) -> uint16 value
    Frame: [addr][func][byte_count][data...][crc_lo][crc_hi]
    """
    byte_count = frame[2]
    data = frame[3 : 3 + byte_count]
    regs: Dict[int, int] = {}
    for i in range(0, len(data), 2):
        reg = (i // 2) + 1
        regs[reg] = struct.unpack(">H", data[i : i + 2])[0]
    return regs

def u16_to_temp(val: int) -> int:
    # your device uses val - 40 for temps
    return int(val) - 40

# -----------------------------
# Main polling loop
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="100Balance BMS → MQTT (Home Assistant discovery)")
    parser.add_argument("--port", default="/dev/tty100Balance", help="Serial port (udev symlink)")
    parser.add_argument("--baud", type=int, default=9600, help="Baud rate")
    parser.add_argument("--mqtt-host", default="localhost", help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--mqtt-user", default="", help="MQTT username")
    parser.add_argument("--mqtt-pass", default="", help="MQTT password")
    parser.add_argument("--interval", type=int, default=10, help="Polling interval (s)")
    parser.add_argument("--cell-count", type=int, default=DEFAULT_CELL_COUNT, help="Number of series cells (7 for your pack)")
    parser.add_argument("--reserve-ah", type=float, default=DEFAULT_RESERVE_AH, help="Reserve capacity (Ah) not to count in time-to-go")
    parser.add_argument("--persist-state", default=DEFAULT_STATE_PATH, help="Persist energy totals to this path (empty disables)")
    parser.add_argument("--persist-every-sec", type=int, default=600, help="How often to write totals to disk (seconds)")
    args = parser.parse_args()

    log.info("🚀 Starting 100Balance BMS MQTT bridge")
    log.info(f"📟 Serial: {args.port} @ {args.baud}, Interval: {args.interval}s")
    log.info(f"📡 MQTT: {args.mqtt_host}:{args.mqtt_port}")
    log.info(f"🔋 Pack: {args.cell_count}S LiFePO4, reserve: {args.reserve_ah}Ah")
    if args.persist_state:
        log.info(f"💾 Energy totals persisted to: {args.persist_state} (every {args.persist_every_sec}s)")
    else:
        log.info("💾 Energy totals persistence disabled")

    # MQTT connect once
    ha = MqttHa(args.mqtt_host, args.mqtt_port, args.mqtt_user, args.mqtt_pass)
    ha.connect()
    publish_discovery(ha, args.cell_count)

    # Energy totals
    energy_in_kwh, energy_out_kwh = (0.0, 0.0)
    last_persist = 0.0
    if args.persist_state:
        energy_in_kwh, energy_out_kwh = load_state(args.persist_state)
        last_persist = time.time()

    # Modbus request: same as your script
    # NOTE: your request starts with 0x81 but response appears from 0x51.
    request = bytes.fromhex("81 03 00 00 00 7F")
    req_crc = compute_crc(request)
    request_frame = request + struct.pack("<H", req_crc)

    # Expect response addr 0x51, func 0x03 (based on your original header search)
    RESP_ADDR = 0x51
    RESP_FUNC = 0x03

    with serial.Serial(args.port, args.baud, timeout=1) as ser:
        while True:
            t0 = time.time()
            try:
                ser.reset_input_buffer()
                ser.write(request_frame)

                # Read a chunk; RTU frames are short but your device sometimes sends extra bytes
                buf = ser.read(512)
                if not buf:
                    time.sleep(args.interval)
                    continue

                frame = find_frame(buf, RESP_ADDR, RESP_FUNC)
                if not frame:
                    # If you ever see a different address, log raw once
                    log.warning(f"Bad/unknown frame (no valid CRC). Raw: {binascii.hexlify(buf).decode()[:120]}...")
                    time.sleep(args.interval)
                    continue

                regs = decode_registers_from_frame(frame)

                # ---- Decode fields (using your mapping + sanity checks) ----
                # Cells: your old script assumed regs 1..16 are mV cell voltages; keep that, but only trust 1..cell_count.
                cell_voltages: Dict[int, float] = {}
                for i in range(1, 17):
                    vraw = regs.get(i)
                    if vraw is None:
                        continue
                    # mV range check
                    if 2500 <= vraw <= 4500:
                        v = round(vraw / 1000.0, 3)
                        cell_voltages[i] = v
                        ha.publish(f"{MQTT_TOPIC_BASE}Cell{i}Voltage", v)
                    else:
                        # still publish nothing; HA retains last value, which is fine
                        pass

                # Temperatures 49..52
                for reg in range(49, 53):
                    val = regs.get(reg)
                    if val is None:
                        continue
                    temp = u16_to_temp(val)
                    if -30 <= temp <= 120:
                        ha.publish(f"{MQTT_TOPIC_BASE}BatteryTemp{reg - 48}", temp)

                # Voltage reg 57 (0.1V)
                voltage = None
                vraw = regs.get(57)
                if vraw is not None:
                    voltage = round(vraw * 0.1, 2)
                    if 10 < voltage < 100:
                        ha.publish(f"{MQTT_TOPIC_BASE}BatteryVoltage", voltage)

                # Current reg 58 (your device: (val - 30000) * 0.1 A)
                current = None
                craw = regs.get(58)
                if craw is not None:
                    current = round((craw - 30000) * 0.1, 2)
                    if abs(current) < 500:
                        ha.publish(f"{MQTT_TOPIC_BASE}Current", current)

                # SOC reg 59 (0.1%)
                soc = None
                sraw = regs.get(59)
                if sraw is not None:
                    soc = round(sraw / 10.0, 1)
                    if 0 <= soc <= 100:
                        ha.publish(f"{MQTT_TOPIC_BASE}SOC", soc)

                # Remaining capacity reg 76 (0.1Ah)
                rem_capacity = None
                rcraw = regs.get(76)
                if rcraw is not None:
                    rem_capacity = round(rcraw * 0.1, 2)
                    if 0 < rem_capacity < 2000:
                        ha.publish(f"{MQTT_TOPIC_BASE}RemainingCapacity", rem_capacity)

                # MOS temp reg 91 (val - 40)
                mos_temp = None
                mraw = regs.get(91)
                if mraw is not None:
                    mos_temp = u16_to_temp(mraw)
                    if -30 <= mos_temp <= 150:
                        ha.publish(f"{MQTT_TOPIC_BASE}MosTemperature", mos_temp)

                # Raw power reg 89 (leave for comparison)
                power_raw_reg89 = None
                praw = regs.get(89)
                if praw is not None and praw < 10000:
                    power_raw_reg89 = int(praw)
                    ha.publish(f"{MQTT_TOPIC_BASE}PowerRawReg89", power_raw_reg89)

                # Signed power from V * I (this is what you should trust)
                power = None
                if voltage is not None and current is not None:
                    # charging positive, discharging negative (matches your Vevor convention in the transition you posted)
                    power = int(round(voltage * current))
                    ha.publish(f"{MQTT_TOPIC_BASE}Power", power)

                    charging = power > 30
                    discharging = power < -30
                    ha.publish(f"{MQTT_TOPIC_BASE}Charging", "ON" if charging else "OFF")
                    ha.publish(f"{MQTT_TOPIC_BASE}Discharging", "ON" if discharging else "OFF")

                    # Helpful directional sensors (always positive magnitude)
                    ha.publish(f"{MQTT_TOPIC_BASE}PowerIn", max(power, 0))
                    ha.publish(f"{MQTT_TOPIC_BASE}PowerOut", max(-power, 0))

                    # Energy accounting (kWh)
                    dt_h = args.interval / 3600.0
                    e_kwh = abs(power) / 1000.0 * dt_h
                    if power > 0:
                        energy_in_kwh += e_kwh
                    elif power < 0:
                        energy_out_kwh += e_kwh
                    ha.publish(f"{MQTT_TOPIC_BASE}EnergyIn", round(energy_in_kwh, 6))
                    ha.publish(f"{MQTT_TOPIC_BASE}EnergyOut", round(energy_out_kwh, 6))

                # Time-to-go (seconds) using actual voltage (correct for your 7S)
                time_to_go_s = None
                if rem_capacity is not None and power is not None and voltage is not None and abs(power) > 10:
                    usable_ah = max(rem_capacity - args.reserve_ah, 0.0)
                    usable_wh = usable_ah * voltage
                    time_to_go_s = int((usable_wh / abs(power)) * 3600)
                    ha.publish(f"{MQTT_TOPIC_BASE}TimeToGo", time_to_go_s)
                    hh = time_to_go_s // 3600
                    mm = (time_to_go_s % 3600) // 60
                    ha.publish(f"{MQTT_TOPIC_BASE}BatteryZeroTime", f"{hh}h {mm}m")

                # Single summary line
                summary = f"V={voltage}V, I={current}A, SOC={soc}%, P={power}W, RC={rem_capacity}Ah, MOS={mos_temp}°C"
                log.info(summary)

                # Persist totals (rate-limited writes)
                if args.persist_state:
                    now = time.time()
                    if now - last_persist >= args.persist_every_sec:
                        save_state(args.persist_state, energy_in_kwh, energy_out_kwh)
                        last_persist = now

            except Exception as e:
                log.error(f"Loop error: {e}")

            # Keep polling cadence stable-ish
            elapsed = time.time() - t0
            sleep_for = max(args.interval - elapsed, 0.2)
            time.sleep(sleep_for)

if __name__ == "__main__":
    main()

