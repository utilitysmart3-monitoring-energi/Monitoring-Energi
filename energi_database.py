from pymodbus.client.sync import ModbusTcpClient
from pymodbus.transaction import ModbusRtuFramer
from supabase import create_client, Client
import paho.mqtt.client as mqtt
import struct
import time
import json
import threading

# --- KONFIGURASI JARINGAN ---
IP_USR = '192.168.7.8'
PORT = 26

# --- KONFIGURASI SUPABASE ---
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# --- KONFIGURASI MQTT ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter"

# --- GLOBAL MEMORY & LOCKING (ANTI RACE CONDITION) ---
LATEST_DATA = {}
data_lock = threading.Lock()

# ==========================================
# DECODER & SAFE READ FUNCTIONS
# ==========================================
def decode_int64(registers):
    try:
        packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
        return struct.unpack('>Q', packed)[0] / 1000.0
    except:
        return 0.0

def decode_float(registers):
    try:
        packed = struct.pack('>HH', registers[0], registers[1])
        return struct.unpack('>f', packed)[0]
    except:
        return 0.0

def decode_int32(registers):
    try:
        packed = struct.pack('>HH', registers[0], registers[1])
        return struct.unpack('>I', packed)[0]
    except:
        return 0

def format_dari_detik(raw_seconds):
    days = raw_seconds // 86400            
    hours = (raw_seconds % 86400) // 3600  
    return f"{days}d {hours}h"

# Fungsi Sakti biar kalau 1 parameter gagal, mesin nggak divonis mati
def safe_read(client, address, count, slave_id, decode_func, default_val):
    try:
        r = client.read_holding_registers(address=address, count=count, unit=slave_id)
        if not r.isError():
            return decode_func(r.registers)
    except:
        pass
    return default_val

# ==========================================
# FUNGSI BACA DATA (WORKER SCADA)
# ==========================================
def get_full_data(client, slave_id):
    DELAY_RS485 = 0.05  # Micro-delay 50ms untuk nafas Gateway USR
    data = {'meter_id': slave_id}
    
    # 1. PING VOLTAGE (Sebagai Penentu Utama Mesin Hidup/Mati)
    is_online = False
    for attempt in range(2):
        try:
            r = client.read_holding_registers(address=3019, count=2, unit=slave_id)
            if not r.isError():
                data['voltage'] = decode_float(r.registers)
                is_online = True
                break
        except:
            pass
        time.sleep(0.3) 
        
    # Kalau Ping Voltage 2x gagal, baru vonis mati
    if not is_online:
        return None 

    time.sleep(DELAY_RS485)

    # 2. Current I1, I2, I3 (Panjang 6, ditangani khusus)
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

    # Mulai dari sini, pakai safe_read. Dijamin aman sentosa!
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

    # Timestamp Watchdog
    data['last_update'] = time.time()

    return data

# ==========================================
# WORKER THREAD (BACKGROUND ENGINE)
# ==========================================
def worker_engine():
    global LATEST_DATA
    print("🚀 [ENGINE] SCADA Worker Started (Modbus RTU Over TCP)...")
    
    modbus_client = ModbusTcpClient(IP_USR, port=PORT, framer=ModbusRtuFramer, timeout=5)
    
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Raspi_Worker_{time.time()}")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start() 
    except:
        mqtt_client = None

    while True:
        if not modbus_client.connect():
            modbus_client.close()
            print("⚠️ [MODBUS] Reconnecting to Gateway USR-IOT...")
            time.sleep(5)
            continue

        for mid in range(1, 19):
            data = get_full_data(modbus_client, mid)
            
            if data:
                with data_lock:
                    LATEST_DATA[mid] = data
                    
                if mqtt_client:
                    try:
                        topic = f"{MQTT_TOPIC_BASE}/{mid}"
                        mqtt_client.publish(topic, json.dumps(data)) # FULL 14 PARAMETER DIKIRIM KE MQTT!
                    except:
                        pass
            else:
                with data_lock:
                    if mid in LATEST_DATA:
                        del LATEST_DATA[mid]
            
            time.sleep(1) # Delay antar mesin

# ==========================================
# MAIN THREAD (SUPABASE LOGGER)
# ==========================================
def main():
    t = threading.Thread(target=worker_engine)
    t.daemon = True
    t.start()

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except:
        print("❌ Gagal Init Supabase")
        return

    GROUPS = {
        "GROUP 1": list(range(1, 7)),
        "GROUP 2": list(range(7, 13)),
        "GROUP 3": list(range(13, 19))
    }

    time.sleep(10)

    while True:
        print(f"\n==========================================================")
        print(f"   SCADA MONITORING SYSTEM - {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")
        
        current_time = time.time()

        for group_name, ids in GROUPS.items():
            print(f"\n💾 [SUPABASE] Processing {group_name}...")
            print("-" * 85)
            print(f"{'ID':<4} | {'VOLT (V)':<8} | {'PART EA(kWh)':<12} | {'TARIF T1':<10} | {'TARIF T2':<10} | {'STATUS'}")
            print("-" * 85)

            for mid in ids:
                d = None
                with data_lock:
                    if mid in LATEST_DATA:
                        # Kick data basi (di atas 120 detik)
                        if current_time - LATEST_DATA[mid].get('last_update', current_time) > 120:
                            del LATEST_DATA[mid] 
                        else:
                            d = LATEST_DATA[mid].copy() 
                
                if d:
                    # ⚠️ FILTER PAYLOAD: HANYA MEMASUKKAN KOLOM YANG ADA DI SKEMA DATABASE!
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
                        status = "✅ SAVED DB"
                        volt_val = f"{d['voltage']:.1f}"
                        pea_val = f"{d['partial_ea']:.1f}"
                        t1_val = f"{d['tarif_t1']:.1f}"
                        t2_val = f"{d['tarif_t2']:.1f}"
                    except Exception as e:
                        status = "❌ DB ERR"
                        volt_val = f"{d['voltage']:.1f}"
                        pea_val = "ERR"
                        t1_val = "ERR"
                        t2_val = "ERR"
                        
                    print(f"{mid:<4} | {volt_val:<8} | {pea_val:<12} | {t1_val:<10} | {t2_val:<10} | {status}")
                
                else:
                    kicked_msg = "⚠️ STALE (KICKED)"
                    print(f"{mid:<4} | {'-':<8} | {'-':<12} | {'-':<10} | {'-':<10} | {kicked_msg}")

            print(f"⏳ Selesai {group_name}. Tidur 5 Menit...")
            time.sleep(300) 

if __name__ == "__main__":
    main()
