from pymodbus.client.sync import ModbusTcpClient
from pymodbus.transaction import ModbusRtuFramer
import paho.mqtt.client as mqtt
import struct
import time
import json
import math  # 🔥 WAJIB ADA BUAT FILTER NaN

# --- KONFIGURASI ---
IP_USR = '192.168.7.8'
PORT = 26
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter"

# --- DECODER & SAFE READ ---
def decode_int64(registers):
    try:
        packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
        return struct.unpack('>Q', packed)[0] / 1000.0
    except: return 0.0

def decode_float(registers):
    try:
        packed = struct.pack('>HH', registers[0], registers[1])
        val = struct.unpack('>f', packed)[0]
        # 🔥 FILTER ANTI-NaN & INFINITY (BIAR JSON GAK CRASH) 🔥
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return round(val, 2)
    except: 
        return 0.0

def decode_int32(registers):
    try:
        packed = struct.pack('>HH', registers[0], registers[1])
        return struct.unpack('>I', packed)[0]
    except: return 0

def safe_read(client, address, count, slave_id, decode_func, default_val):
    try:
        r = client.read_holding_registers(address=address, count=count, unit=slave_id)
        if not r.isError(): return decode_func(r.registers)
    except: pass
    return default_val

# --- WORKER BACA DATA ---
def get_full_data(client, slave_id):
    DELAY_RS485 = 0.2 # 🔥 200ms Biar Gateway rileks nggak tersedak
    data = {'meter_id': slave_id}
    
    # 1. PING VOLTAGE (Cek Hidup/Mati)
    is_online = False
    for attempt in range(2):
        try:
            r = client.read_holding_registers(address=3019, count=2, unit=slave_id)
            if not r.isError():
                data['voltage'] = decode_float(r.registers)
                is_online = True
                break
        except: pass
        time.sleep(0.5) 
        
    if not is_online: return None 

    time.sleep(DELAY_RS485)

    # 2. Current I1, I2, I3
    try:
        r = client.read_holding_registers(address=2999, count=6, unit=slave_id)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0
    except:
        data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

    time.sleep(DELAY_RS485)

    # 3. Sisa Parameter Lengkap (14 Item)
    data['active_p'] = safe_read(client, 3053, 2, slave_id, decode_float, 0.0)
    time.sleep(DELAY_RS485)
    data['reactive_q'] = safe_read(client, 3059, 2, slave_id, decode_float, 0.0)
    time.sleep(DELAY_RS485)
    data['apparent_s'] = safe_read(client, 3067, 2, slave_id, decode_float, 0.0)
    time.sleep(DELAY_RS485)
    data['pf'] = safe_read(client, 3083, 2, slave_id, decode_float, 0.0)
    time.sleep(DELAY_RS485)
    data['freq'] = safe_read(client, 3109, 2, slave_id, decode_float, 0.0)
    time.sleep(DELAY_RS485)
    data['total_ea'] = safe_read(client, 3203, 4, slave_id, decode_int64, 0.0)
    time.sleep(DELAY_RS485)
    data['partial_ea'] = safe_read(client, 3255, 4, slave_id, decode_int64, 0.0)
    time.sleep(DELAY_RS485)
    data['tarif_t1'] = safe_read(client, 4195, 4, slave_id, decode_int64, 0.0)
    time.sleep(DELAY_RS485)
    data['tarif_t2'] = safe_read(client, 4199, 4, slave_id, decode_int64, 0.0)
    time.sleep(DELAY_RS485)
    data['total_er'] = safe_read(client, 3219, 4, slave_id, decode_int64, 0.0)
    time.sleep(DELAY_RS485)
    data['partial_er'] = safe_read(client, 3271, 4, slave_id, decode_int64, 0.0)
    time.sleep(DELAY_RS485)
    data['op_time'] = safe_read(client, 2003, 2, slave_id, decode_int32, 0)

    data['last_update'] = time.time()
    return data

# --- MAIN LOOP ---
def main():
    print("🚀 [REALTIME ENGINE] Mulai membaca Modbus...")
    modbus_client = ModbusTcpClient(IP_USR, port=PORT, framer=ModbusRtuFramer, timeout=5)
    
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Publisher_{time.time()}")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start() 
        print("✅ Terhubung ke MQTT Broker!")
    except:
        mqtt_client = None

    while True:
        if not modbus_client.connect():
            modbus_client.close()
            print("⚠️ Reconnecting ke Gateway USR...")
            time.sleep(5)
            continue

        for mid in range(1, 19):
            data = get_full_data(modbus_client, mid)
            if data and mqtt_client:
                topic = f"{MQTT_TOPIC_BASE}/{mid}"
                mqtt_client.publish(topic, json.dumps(data))
                print(f"📡 ID {mid} -> MQTT Published")
            elif not data:
                print(f"❌ ID {mid} -> OFFLINE")
            
            time.sleep(1.5) # 🔥 Jeda 1.5 detik antar mesin biar aliran data super stabil

if __name__ == "__main__":
    main()
