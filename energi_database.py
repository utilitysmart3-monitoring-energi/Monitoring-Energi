from pymodbus.client import ModbusTcpClient
from supabase import create_client, Client
import paho.mqtt.client as mqtt
import struct
import time
import os
import json
import threading

# --- KONFIGURASI ---
IP_USR = '192.168.5.20'
PORT = 502

# Supabase
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# MQTT
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter"

# --- GLOBAL MEMORY (TEMPAT PENAMPUNGAN DATA) ---
# Format: { 1: {data_lengkap}, 2: {data_lengkap}, ... }
LATEST_DATA = {}

# --- DECODER ---
def decode_int64(registers):
    packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
    return struct.unpack('>Q', packed)[0] / 1000.0

def decode_float(registers):
    packed = struct.pack('>HH', registers[0], registers[1])
    return struct.unpack('>f', packed)[0]

def get_full_data(client, slave_id):
    """Fungsi Baca Modbus (Hanya dipanggil oleh Worker Thread)"""
    try:
        data = {'meter_id': slave_id}
        
        # Gunakan 'slave' atau 'unit' atau 'device_id' sesuai library yg jalan
        # Kita pakai device_id karena tadi sukses disitu
        kwargs = {'device_id': slave_id} 

        # Voltage
        r = client.read_holding_registers(address=3019, count=2, **kwargs)
        data['voltage'] = decode_float(r.registers) if not r.isError() else 0.0

        # Current
        r = client.read_holding_registers(address=2999, count=6, **kwargs)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

        # Power
        r_p = client.read_holding_registers(address=3053, count=2, **kwargs)
        data['active_p'] = decode_float(r_p.registers) if not r_p.isError() else 0.0
        
        r_q = client.read_holding_registers(address=3059, count=2, **kwargs)
        data['reactive_q'] = decode_float(r_q.registers) if not r_q.isError() else 0.0

        r_s = client.read_holding_registers(address=3067, count=2, **kwargs)
        data['apparent_s'] = decode_float(r_s.registers) if not r_s.isError() else 0.0

        # PF & Freq
        r_pf = client.read_holding_registers(address=3083, count=2, **kwargs)
        data['pf'] = decode_float(r_pf.registers) if not r_pf.isError() else 0.0

        r_fr = client.read_holding_registers(address=3109, count=2, **kwargs)
        data['freq'] = decode_float(r_fr.registers) if not r_fr.isError() else 0.0

        # Energi
        r_tot = client.read_holding_registers(address=3203, count=4, **kwargs)
        data['total_ea'] = decode_int64(r_tot.registers) if not r_tot.isError() else 0.0
        
        r_toter = client.read_holding_registers(address=3219, count=4, **kwargs)
        data['total_er'] = decode_int64(r_toter.registers) if not r_toter.isError() else 0.0

        r_par = client.read_holding_registers(address=3255, count=4, **kwargs)
        data['partial_ea'] = decode_int64(r_par.registers) if not r_par.isError() else 0.0

        r_parer = client.read_holding_registers(address=3271, count=4, **kwargs)
        data['partial_er'] = decode_int64(r_parer.registers) if not r_parer.isError() else 0.0

        # Tarif
        r_t1 = client.read_holding_registers(address=4195, count=4, **kwargs)
        data['tarif_t1'] = decode_int64(r_t1.registers) if not r_t1.isError() else 0.0

        r_t2 = client.read_holding_registers(address=4199, count=4, **kwargs)
        data['tarif_t2'] = decode_int64(r_t2.registers) if not r_t2.isError() else 0.0

        data['op_time'] = "Running"
        return data
    except:
        return None

# ==========================================
# WORKER THREAD (SI PEKERJA KERAS)
# Tugas: Baca Modbus Terus-menerus -> Simpan ke Memori -> Kirim MQTT
# ==========================================
def worker_engine():
    global LATEST_DATA
    print("🚀 [ENGINE] Worker Thread Started (Modbus + MQTT)...")
    
    # 1. Koneksi Modbus (Cuma 1 Koneksi untuk selamanya)
    modbus_client = ModbusTcpClient(IP_USR, port=PORT)
    
    # 2. Koneksi MQTT
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Raspi_Worker_{time.time()}")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except:
        print("⚠️ [MQTT] Connect Fail (Will retry)")

    while True:
        # Cek koneksi Modbus
        if not modbus_client.connected:
            modbus_client.connect()
            time.sleep(1)

        # Loop Scan ID 1-18
        for mid in range(1, 19):
            # A. BACA DATA DARI ALAT
            data = get_full_data(modbus_client, mid)
            
            if data:
                # B. SIMPAN KE MEMORI GLOBAL (Biar Supabase bisa ambil nanti)
                LATEST_DATA[mid] = data
                
                # C. KIRIM MQTT (Realtime)
                try:
                    topic = f"{MQTT_TOPIC_BASE}/{mid}"
                    mqtt_client.publish(topic, json.dumps(data))
                except:
                    pass # MQTT error jangan bikin stop modbus
            
            # Jeda dikit biar napas (0.2 detik per mesin)
            time.sleep(0.2)

# ==========================================
# MAIN THREAD (SI BOS SANTAI)
# Tugas: Ambil data dari Memori -> Kirim Supabase -> Tidur
# ==========================================
def main():
    # 1. Jalankan Worker di Background
    t = threading.Thread(target=worker_engine)
    t.daemon = True
    t.start()

    # 2. Inisialisasi Supabase
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Grouping
    GROUPS = {
        "GROUP 1": list(range(1, 7)),
        "GROUP 2": list(range(7, 13)),
        "GROUP 3": list(range(13, 19))
    }

    print("⏳ Menunggu Worker mengisi data awal (10 detik)...")
    time.sleep(10) # Kasih waktu worker baca data dulu

    while True:

        print(f"==========================================================")
        print(f"   SMART MONITORING SYSTEM (SINGLE CONNECTION)")
        print(f"   [REALTIME]: MQTT ID 1-18 (Continuous)")
        print(f"   [LOGGING]: Supabase Cycle (15 Mins)")
        print(f"   Waktu: {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")

        # --- LOGIC SUPABASE (AMBIL DARI MEMORI) ---
        for group_name, ids in GROUPS.items():
            print(f"\n💾 [SUPABASE] Processing {group_name}...")
            print("-" * 50)
            print(f"{'ID':<4} | {'VOLT':<7} | {'STATUS DB':<15}")
            print("-" * 50)

            for mid in ids:
                # Cek apakah ada data di memori?
                if mid in LATEST_DATA:
                    d = LATEST_DATA[mid]
                    
                    # Kirim ke Supabase
                    payload = {
                        "meter_id": d['meter_id'],
                        "partial_ea": d['partial_ea'],
                        "voltage": d['voltage'],
                        "current_i1": d['current_i1'],
                        "current_i2": d['current_i2'],
                        "current_i3": d['current_i3'],
                        "tarif_t1": d['tarif_t1'],
                        "tarif_t2": d['tarif_t2']
                    }
                    try:
                        supabase.table("energi_db").insert(payload).execute()
                        status = "✅ SAVED"
                        volt_val = f"{d['voltage']:.1f}"
                    except:
                        status = "❌ ERR API"
                        volt_val = "ERR"
                else:
                    status = "⚠️ NO DATA" # Belum terbaca oleh Worker
                    volt_val = "-"
                
                print(f"{mid:<4} | {volt_val:<7} | {status}")

            print(f"\n⏳ Selesai {group_name}. Tidur 5 Menit (MQTT Tetap Jalan)...")
            time.sleep(300) # Tidur 5 Menit sebelum lanjut grup berikutnya

if __name__ == "__main__":
    main()
