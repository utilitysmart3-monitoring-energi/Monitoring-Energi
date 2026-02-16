from pymodbus.client import ModbusTcpClient
from supabase import create_client, Client
import paho.mqtt.client as mqtt
import struct
import time
import os
import json
import threading  # <--- INI KUNCI BIAR BISA JALAN BARENG

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

# --- DECODER (Sama kayak sebelumnya) ---
def decode_int64(registers):
    packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
    return struct.unpack('>Q', packed)[0] / 1000.0

def decode_float(registers):
    packed = struct.pack('>HH', registers[0], registers[1])
    return struct.unpack('>f', packed)[0]

def get_full_data(client, slave_id):
    """Ambil data 16 parameter (Dipakai oleh MQTT & Supabase)"""
    try:
        data = {'meter_id': slave_id}
        
        # Gunakan device_id sesuai request sebelumnya
        # Voltage
        r = client.read_holding_registers(address=3019, count=2, slave=slave_id)
        data['voltage'] = decode_float(r.registers) if not r.isError() else 0.0

        # Current
        r = client.read_holding_registers(address=2999, count=6, slave=slave_id)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

        # Power
        r_p = client.read_holding_registers(address=3053, count=2, slave=slave_id)
        data['active_p'] = decode_float(r_p.registers) if not r_p.isError() else 0.0
        
        r_q = client.read_holding_registers(address=3059, count=2, slave=slave_id)
        data['reactive_q'] = decode_float(r_q.registers) if not r_q.isError() else 0.0

        r_s = client.read_holding_registers(address=3067, count=2, slave=slave_id)
        data['apparent_s'] = decode_float(r_s.registers) if not r_s.isError() else 0.0

        # PF & Freq
        r_pf = client.read_holding_registers(address=3083, count=2, slave=slave_id)
        data['pf'] = decode_float(r_pf.registers) if not r_pf.isError() else 0.0

        r_fr = client.read_holding_registers(address=3109, count=2, slave=slave_id)
        data['freq'] = decode_float(r_fr.registers) if not r_fr.isError() else 0.0

        # Energi (Total & Partial)
        r_tot = client.read_holding_registers(address=3203, count=4, slave=slave_id)
        data['total_ea'] = decode_int64(r_tot.registers) if not r_tot.isError() else 0.0
        
        r_toter = client.read_holding_registers(address=3219, count=4, slave=slave_id)
        data['total_er'] = decode_int64(r_toter.registers) if not r_toter.isError() else 0.0

        r_par = client.read_holding_registers(address=3255, count=4, slave=slave_id)
        data['partial_ea'] = decode_int64(r_par.registers) if not r_par.isError() else 0.0

        r_parer = client.read_holding_registers(address=3271, count=4, slave=slave_id)
        data['partial_er'] = decode_int64(r_parer.registers) if not r_parer.isError() else 0.0

        # Tarif
        r_t1 = client.read_holding_registers(address=4195, count=4, slave=slave_id)
        data['tarif_t1'] = decode_int64(r_t1.registers) if not r_t1.isError() else 0.0

        r_t2 = client.read_holding_registers(address=4199, count=4, slave=slave_id)
        data['tarif_t2'] = decode_int64(r_t2.registers) if not r_t2.isError() else 0.0

        data['op_time'] = "Running"
        return data
    except:
        return None

# ==========================================
# JALUR CEPAT: WORKER KHUSUS MQTT (Thread)
# ==========================================
def mqtt_worker():
    """
    Fungsi ini berjalan sendiri di background.
    Tugasnya: Scan ID 1-18 secepat mungkin & kirim ke MQTT.
    Tidak peduli dengan Supabase atau sleep 5 menit.
    """
    print("🚀 [THREAD] MQTT Streamer Started!")
    
    # 1. Bikin Koneksi Modbus SENDIRI (Biar gak rebutan sama Supabase)
    client_fast = ModbusTcpClient(IP_USR, port=PORT)
    client_fast.connect()

    # 2. Bikin Koneksi MQTT
    mqtt_fast = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Raspi_Streamer_{time.time()}")
    try:
        mqtt_fast.connect(MQTT_BROKER, MQTT_PORT, 60)
    except:
        print("⚠️ [THREAD] MQTT Connect Fail")

    while True:
        # Scan Semua Mesin 1 s/d 18 Tanpa Henti
        for mid in range(1, 19):
            if not client_fast.connected: client_fast.connect()
            
            # Ambil Data
            data = get_full_data(client_fast, mid)
            
            if data:
                # Kirim MQTT
                try:
                    topic = f"{MQTT_TOPIC_BASE}/{mid}"
                    mqtt_fast.publish(topic, json.dumps(data))
                    # print(f"📡 [MQTT] ID {mid} Sent") # Uncomment kalau mau log rame
                except:
                    pass
            
            # Jeda dikit biar CPU gak 100% (0.5 detik per mesin)
            time.sleep(0.5) 
        
        # Setelah scan 1-18 selesai, ulang lagi langsung!

# ==========================================
# JALUR LAMBAT: MAIN PROGRAM (Supabase)
# ==========================================
def main():
    # 1. NYALAKAN THREAD MQTT DULUAN
    # Thread ini akan jalan 'di samping' program utama
    t = threading.Thread(target=mqtt_worker)
    t.daemon = True # Biar kalau program diclose, thread ikut mati
    t.start()

    # 2. INISIALISASI SUPABASE CLIENT (Jalur Utama)
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    client_slow = ModbusTcpClient(IP_USR, port=PORT)
    
    # Grouping
    GROUP_1 = list(range(1, 7))
    GROUP_2 = list(range(7, 13))
    GROUP_3 = list(range(13, 19))

    while True:
        os.system('clear')
        print(f"==========================================================")
        print(f"   DUAL CORE MONITORING SYSTEM")
        print(f"   [BACKGROUND]: MQTT Streaming Realtime (ID 1-18)")
        print(f"   [MAIN]: Supabase Logging (Cycle 15 Mins)")
        print(f"   Waktu: {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")

        if not client_slow.connect():
            print("❌ Main Modbus Disconnected...")
            time.sleep(5)
            continue

        # --- FUNGSI KHUSUS LOG KE SUPABASE ---
        def log_to_supabase(ids, grup_name):
            print(f"\n💾 [SUPABASE] Saving {grup_name}...")
            print("-" * 50)
            print(f"{'ID':<4} | {'STATUS DB':<15}")
            print("-" * 50)
            
            for slave_id in ids:
                # Ambil data pake client_slow
                data = get_full_data(client_slow, slave_id)
                status = "SKIP"
                
                if data:
                    payload = {
                        "meter_id": data['meter_id'],
                        "partial_ea": data['partial_ea'],
                        "voltage": data['voltage'],
                        "current_i1": data['current_i1'],
                        "current_i2": data['current_i2'],
                        "current_i3": data['current_i3'],
                        "tarif_t1": data['tarif_t1'],
                        "tarif_t2": data['tarif_t2']
                    }
                    try:
                        supabase.table("energi_db").insert(payload).execute()
                        status = "✅ SAVED"
                    except:
                        status = "❌ ERR DB"
                
                print(f"{slave_id:<4} | {status}")

        # --- SIKLUS UTAMA (Tetap 15 Menit) ---
        
        # JALAN GRUP 1
        log_to_supabase(GROUP_1, "GROUP 1")
        print("\n⏳ Main Thread Sleep 5 Menit (MQTT Tetap Jalan)...")
        time.sleep(300)

        # JALAN GRUP 2
        log_to_supabase(GROUP_2, "GROUP 2")
        print("\n⏳ Main Thread Sleep 5 Menit (MQTT Tetap Jalan)...")
        time.sleep(300)

        # JALAN GRUP 3
        log_to_supabase(GROUP_3, "GROUP 3")
        print("\n⏳ Main Thread Sleep 5 Menit (Siklus Selesai)...")
        time.sleep(300)

if __name__ == "__main__":
    main()
