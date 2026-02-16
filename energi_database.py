from pymodbus.client import ModbusTcpClient
from supabase import create_client, Client
import paho.mqtt.client as mqtt
import struct
import time
import os
import json

# --- KONFIGURASI MODBUS ---
IP_USR = '192.168.5.20'
PORT = 502

# --- KONFIGURASI SUPABASE ---
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# --- KONFIGURASI MQTT (HIVEMQ) ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter"

# --- INISIALISASI CLIENT SUPABASE ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- PEMBAGIAN KELOMPOK ID ---
GROUP_1 = list(range(1, 7))   # ID 1 - 6
GROUP_2 = list(range(7, 13))  # ID 7 - 12
GROUP_3 = list(range(13, 19)) # ID 13 - 18

# --- DECODER FUNGSI ---
def decode_int64(registers):
    # Untuk Energi (kWh) - 4 Register
    # Big Endian: Register[0] adalah High Word
    packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
    return struct.unpack('>Q', packed)[0] / 1000.0

def decode_float(registers):
    # Untuk Volt, Ampere, Power - 2 Register
    packed = struct.pack('>HH', registers[0], registers[1])
    return struct.unpack('>f', packed)[0]

def get_full_data(client, slave_id):
    """
    Mengambil SEMUA 16 Parameter dari Power Meter.
    Return: Dictionary data lengkap atau None jika error.
    Fix: Menggunakan Keyword Arguments (address, count, slave) untuk Pymodbus V3.
    """
    try:
        data = {}
        data['meter_id'] = slave_id

        # 1. BACA VOLTAGE (3020) - Float
        r = client.read_holding_registers(address=3019, count=2, slave=slave_id)
        data['voltage'] = decode_float(r.registers) if not r.isError() else 0.0

        # 2. BACA CURRENT I1, I2, I3 (3000-3005) - Float
        r = client.read_holding_registers(address=2999, count=6, slave=slave_id)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

        # 3. BACA POWER (Active P, Reactive Q, Apparent S) - Float
        # P=3054, Q=3060, S=3068
        r_p = client.read_holding_registers(address=3053, count=2, slave=slave_id)
        data['active_p'] = decode_float(r_p.registers) if not r_p.isError() else 0.0
        
        r_q = client.read_holding_registers(address=3059, count=2, slave=slave_id)
        data['reactive_q'] = decode_float(r_q.registers) if not r_q.isError() else 0.0

        r_s = client.read_holding_registers(address=3067, count=2, slave=slave_id)
        data['apparent_s'] = decode_float(r_s.registers) if not r_s.isError() else 0.0

        # 4. PF (3084) & Freq (3110) - Float
        r_pf = client.read_holding_registers(address=3083, count=2, slave=slave_id)
        data['pf'] = decode_float(r_pf.registers) if not r_pf.isError() else 0.0

        r_fr = client.read_holding_registers(address=3109, count=2, slave=slave_id)
        data['freq'] = decode_float(r_fr.registers) if not r_fr.isError() else 0.0

        # 5. ENERGI (INT64 - 4 Register)
        # Total Ea (3204), Total Er (3220), Partial Ea (3256), Partial Er (3272)
        r_tot = client.read_holding_registers(address=3203, count=4, slave=slave_id)
        data['total_ea'] = decode_int64(r_tot.registers) if not r_tot.isError() else 0.0
        
        r_toter = client.read_holding_registers(address=3219, count=4, slave=slave_id)
        data['total_er'] = decode_int64(r_toter.registers) if not r_toter.isError() else 0.0

        r_par = client.read_holding_registers(address=3255, count=4, slave=slave_id)
        data['partial_ea'] = decode_int64(r_par.registers) if not r_par.isError() else 0.0

        r_parer = client.read_holding_registers(address=3271, count=4, slave=slave_id)
        data['partial_er'] = decode_int64(r_parer.registers) if not r_parer.isError() else 0.0

        # 6. TARIF (INT64) - T1(4196), T2(4200)
        r_t1 = client.read_holding_registers(address=4195, count=4, slave=slave_id)
        data['tarif_t1'] = decode_int64(r_t1.registers) if not r_t1.isError() else 0.0

        r_t2 = client.read_holding_registers(address=4199, count=4, slave=slave_id)
        data['tarif_t2'] = decode_int64(r_t2.registers) if not r_t2.isError() else 0.0

        # 7. Op Time
        data['op_time'] = "Running" 

        return data

    except Exception as e:
        # Uncomment baris ini jika ingin debugging error spesifik
        # print(f"⚠️ Debug Error ID {slave_id}: {e}")
        return None

def process_group(modbus_client, mqtt_client, group_ids, group_name):
    print(f"\n🚀 {group_name} (ID {group_ids[0]}-{group_ids[-1]})")
    print("-" * 75)
    print(f"{'ID':<3} | {'Tot kWh':<10} | {'Volt':<7} | {'SUPABASE':<10} | {'MQTT':<10}")
    print("-" * 75)

    for slave_id in group_ids:
        # 1. AMBIL DATA LENGKAP (16 Parameter)
        full_data = get_full_data(modbus_client, slave_id)
        
        status_supa = "SKIP"
        status_mqtt = "SKIP"
        display_kwh = "-"
        display_volt = "-"

        if full_data:
            display_kwh = f"{full_data['total_ea']:.2f}"
            display_volt = f"{full_data['voltage']:.1f}"

            # --- TASK A: KIRIM SUPABASE (Hanya Data Penting) ---
            supa_payload = {
                "meter_id": full_data['meter_id'],
                "partial_ea": full_data['partial_ea'],
                "voltage": full_data['voltage'],
                "current_i1": full_data['current_i1'],
                "current_i2": full_data['current_i2'],
                "current_i3": full_data['current_i3'],
                "tarif_t1": full_data['tarif_t1'],
                "tarif_t2": full_data['tarif_t2']
            }
            try:
                supabase.table("energi_db").insert(supa_payload).execute()
                status_supa = "✅ OK"
            except:
                status_supa = "❌ ERR"

            # --- TASK B: KIRIM MQTT (Data Lengkap) ---
            try:
                topic = f"{MQTT_TOPIC_BASE}/{slave_id}"
                mqtt_payload = json.dumps(full_data)
                mqtt_client.publish(topic, mqtt_payload) # Non-blocking QoS 0
                status_mqtt = "✅ SENT"
            except:
                status_mqtt = "❌ FAIL"
            
            # Print Log
            print(f"{slave_id:<3} | {display_kwh:>10} | {display_volt:>7} | {status_supa:<10} | {status_mqtt:<10}")
        else:
            print(f"{slave_id:<3} | {'ERROR':>10} | {'-':>7} | {status_supa:<10} | {status_mqtt:<10}")

def main():
    # 1. KONEKSI MODBUS
    mb_client = ModbusTcpClient(IP_USR, port=PORT)
    
    # 2. KONEKSI MQTT (Fix versi API 2.0)
    # Gunakan CallbackAPIVersion.VERSION2 untuk library Paho MQTT v2.x
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Raspi_Logger_{time.time()}")
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start() # Jalankan background thread untuk MQTT
        print(f"📡 MQTT Connected to {MQTT_BROKER}")
    except Exception as e:
        print(f"⚠️ MQTT Connection Failed: {e}")

    while True:
        os.system('clear')
        print(f"==========================================================")
        print(f"   INDUSTRIAL ENERGY MONITOR (SUPABASE + MQTT)")
        print(f"   Waktu: {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")

        if not mb_client.connect():
            print("❌ Modbus Gateway Disconnected. Retrying 10s...")
            time.sleep(10)
            continue

        # --- SIKLUS 15 MENIT ---
        
        # 1. GRUP 1 (5 Menit)
        process_group(mb_client, mqtt_client, GROUP_1, "GRUP 1")
        print("\n⏳ Standby 5 Menit (Grup 1 Selesai)...")
        time.sleep(300) 

        # 2. GRUP 2 (5 Menit)
        process_group(mb_client, mqtt_client, GROUP_2, "GRUP 2")
        print("\n⏳ Standby 5 Menit (Grup 2 Selesai)...")
        time.sleep(300)

        # 3. GRUP 3 (5 Menit)
        process_group(mb_client, mqtt_client, GROUP_3, "GRUP 3")
        print("\n⏳ Standby 5 Menit (Siklus Selesai)...")
        time.sleep(300)

        # Re-connect check MQTT
        if not mqtt_client.is_connected():
            try: mqtt_client.reconnect()
            except: pass

if __name__ == "__main__":
    main()
