import threading
import time
import json
import struct
import math
import paho.mqtt.client as mqtt
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.transaction import ModbusRtuFramer
from supabase import create_client, Client

# ==============================================================================
# 🔧 KONFIGURASI GLOBAL
# ==============================================================================
# --- Supabase ---
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# --- Modbus TCP (USR Gateway) ---
IP_USR = '192.168.7.8'
PORT = 26

# --- MQTT ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter"

# --- Shared Memory (Antar Thread) ---
LATEST_DATA = {}
data_lock = threading.Lock()

# ==============================================================================
# 🛠️ FUNGSI DECODER MODBUS (ANTI-CRASH)
# ==============================================================================
def decode_int64(registers):
    try:
        packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
        return struct.unpack('>Q', packed)[0] / 1000.0
    except: return 0.0

def decode_float(registers):
    try:
        packed = struct.pack('>HH', registers[0], registers[1])
        val = struct.unpack('>f', packed)[0]
        # 🔥 FILTER ANTI-NaN & INFINITY (BIAR JSON/SUPABASE GAK CRASH)
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

# ==============================================================================
# 📡 WORKER 1: BACA MODBUS & PUBLISH MQTT (REALTIME)
# ==============================================================================
def get_full_data(client, slave_id):
    DELAY_RS485 = 0.2 # 200ms Biar Gateway rileks nggak tersedak
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

def modbus_mqtt_worker():
    print("🚀 [REALTIME ENGINE] Mulai membaca Modbus...")
    modbus_client = ModbusTcpClient(IP_USR, port=PORT, framer=ModbusRtuFramer, timeout=5)
    
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Publisher_{time.time()}")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start() 
        print("✅ Terhubung ke MQTT Broker!")
    except:
        print("❌ Gagal terhubung ke MQTT Broker.")
        mqtt_client = None

    while True:
        if not modbus_client.connect():
            modbus_client.close()
            print("⚠️ Reconnecting ke Gateway USR (192.168.7.8)...")
            time.sleep(5)
            continue

        for mid in range(1, 19):
            data = get_full_data(modbus_client, mid)
            
            if data:
                # 1. Simpan ke Memori Internal (Untuk dikirim ke Supabase oleh Thread 2)
                with data_lock:
                    LATEST_DATA[mid] = data

                # 2. Publish ke MQTT (Untuk Dashboard HTML realtime)
                if mqtt_client:
                    topic = f"{MQTT_TOPIC_BASE}/{mid}"
                    mqtt_client.publish(topic, json.dumps(data))
                    print(f"📡 ID {mid} -> MQTT Published & Memori Diperbarui")
            else:
                print(f"❌ ID {mid} -> OFFLINE")
            
            time.sleep(1.5) # 🔥 Jeda 1.5 detik antar mesin biar aliran data super stabil

# ==============================================================================
# 💾 WORKER 2: SUPABASE LOGGER (BACKGROUND DATA UPLOAD)
# ==============================================================================
def supabase_worker():
    print("🚀 [SUPABASE ENGINE] Mulai rutinitas rekam Tarif & Energi...")
    
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Supabase Terhubung!")
    except Exception as e:
        print(f"❌ Gagal Init Supabase: {e}")
        return

    GROUPS = {
        "GROUP 1": list(range(1, 7)),
        "GROUP 2": list(range(7, 13)),
        "GROUP 3": list(range(13, 19))
    }

    # Tunggu ngumpulin data di memori dulu selama 20 detik
    time.sleep(20)

    while True:
        print(f"\n==========================================================")
        print(f"   SUPABASE LOGGER - {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")
        
        current_time = time.time()

        for group_name, ids in GROUPS.items():
            print(f"\n💾 Processing {group_name}...")
            print("-" * 80)
            print(f"{'ID':<4} | {'VOLT (V)':<8} | {'PART EA':<10} | {'TARIF T1':<10} | {'TARIF T2':<10} | {'STATUS'}")
            print("-" * 80)

            for mid in ids:
                d = None
                with data_lock:
                    if mid in LATEST_DATA:
                        # Jika data lebih dari 120 detik (basi/offline lama), hapus aja
                        if current_time - LATEST_DATA[mid].get('last_update', current_time) > 120:
                            del LATEST_DATA[mid] 
                        else:
                            d = LATEST_DATA[mid].copy() 
                
                if d:
                    # PAYLOAD KETAT KHUSUS SUPABASE (SESUAI TABEL)
                    payload = {
                        "meter_id": d['meter_id'],
                        "partial_ea": d['partial_ea'],
                        "tarif_t1": d['tarif_t1'],
                        "tarif_t2": d['tarif_t2'],
                        "voltage": d['voltage'],
                        "current_i1": d['current_i1'],
                        "current_i2": d['current_i2'],
                        "current_i3": d['current_i3']
                    }
                    
                    try:
                        supabase.table("energi_db").insert(payload).execute()
                        status = "✅ DB SAVED"
                        volt_val = f"{d['voltage']:.1f}"
                        pea_val = f"{d['partial_ea']:.1f}"
                        t1_val = f"{d['tarif_t1']:.1f}"
                        t2_val = f"{d['tarif_t2']:.1f}"
                    except Exception as e:
                        status = "❌ DB ERR"
                        volt_val = pea_val = t1_val = t2_val = "ERR"
                        
                    print(f"{mid:<4} | {volt_val:<8} | {pea_val:<10} | {t1_val:<10} | {t2_val:<10} | {status}")
                else:
                    print(f"{mid:<4} | {'-':<8} | {'-':<10} | {'-':<10} | {'-':<10} | ⚠️ DATA NOT READY")

            print(f"⏳ Selesai {group_name}. Tidur 5 Menit...")
            time.sleep(300)

# ==============================================================================
# 🔥 MAIN EXECUTION: JALANKAN KEDUA THREAD BERSAMAAN
# ==============================================================================
if __name__ == "__main__":
    print("==========================================================")
    print("⚡ MEMULAI MASTER ENGINE (MODBUS + MQTT + SUPABASE) ⚡")
    print("==========================================================")
    
    # Menjalankan Worker Modbus & MQTT di Thread Pertama
    t1 = threading.Thread(target=modbus_mqtt_worker, daemon=True)
    t1.start()

    # Menjalankan Worker Supabase di Thread Kedua
    t2 = threading.Thread(target=supabase_worker, daemon=True)
    t2.start()

    # Biarkan program utama tetap hidup
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Program dihentikan oleh user. Selamat tinggal!")
