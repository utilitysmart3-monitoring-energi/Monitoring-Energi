from pymodbus.client.sync import ModbusTcpClient
from pymodbus.transaction import ModbusRtuFramer
from supabase import create_client, Client
import paho.mqtt.client as mqtt
import struct
import time
import json
import threading

# --- KONFIGURASI JARINGAN ---
IP_USR = '192.168.7.8'  # IP Gateway USR
PORT = 26               # Port Gateway USR

# --- KONFIGURASI SUPABASE ---
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# --- KONFIGURASI MQTT ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter"

# --- GLOBAL MEMORY ---
LATEST_DATA = {}

# --- DECODER FUNCTIONS ---
def decode_int64(registers):
    """Mengubah 4 register (16-bit) menjadi nilai 64-bit float (kWh)"""
    try:
        packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
        return struct.unpack('>Q', packed)[0] / 1000.0
    except:
        return 0.0

def decode_float(registers):
    """Mengubah 2 register (16-bit) menjadi nilai 32-bit float"""
    try:
        packed = struct.pack('>HH', registers[0], registers[1])
        return struct.unpack('>f', packed)[0]
    except:
        return 0.0

# --- FUNGSI BACA DATA (WORKER) ---
def get_full_data(client, slave_id):
    """Membaca semua register penting dari satu meteran"""
    try:
        data = {'meter_id': slave_id}
        
        # PENTING: Gunakan 'unit=slave_id' untuk Pymodbus 2.5.3
        
        # 1. Voltage (Register 3019, Panjang 2)
        # Mapping asli 3020 -> dikurangi 1 jadi 3019
        r = client.read_holding_registers(address=3019, count=2, unit=slave_id)
        if not r.isError():
            data['voltage'] = decode_float(r.registers)
        else:
            return None # Anggap offline jika voltage gagal

        # 2. Current (Register 2999, Panjang 6) -> I1, I2, I3
        r = client.read_holding_registers(address=2999, count=6, unit=slave_id)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

        # 3. Active Power (Register 3053, Panjang 2)
        r_p = client.read_holding_registers(address=3053, count=2, unit=slave_id)
        data['active_p'] = decode_float(r_p.registers) if not r_p.isError() else 0.0
        
        # 4. Reactive Power (Register 3059, Panjang 2)
        r_q = client.read_holding_registers(address=3059, count=2, unit=slave_id)
        data['reactive_q'] = decode_float(r_q.registers) if not r_q.isError() else 0.0

        # 5. Apparent Power (Register 3067, Panjang 2)
        r_s = client.read_holding_registers(address=3067, count=2, unit=slave_id)
        data['apparent_s'] = decode_float(r_s.registers) if not r_s.isError() else 0.0

        # 6. PF & Freq
        r_pf = client.read_holding_registers(address=3083, count=2, unit=slave_id)
        data['pf'] = decode_float(r_pf.registers) if not r_pf.isError() else 0.0

        r_fr = client.read_holding_registers(address=3109, count=2, unit=slave_id)
        data['freq'] = decode_float(r_fr.registers) if not r_fr.isError() else 0.0

        # 7. Energy Total (Register 3203, Panjang 4)
        r_tot = client.read_holding_registers(address=3203, count=4, unit=slave_id)
        data['total_ea'] = decode_int64(r_tot.registers) if not r_tot.isError() else 0.0
        
        # 8. Partial Energy (Register 3255, Panjang 4)
        r_par = client.read_holding_registers(address=3255, count=4, unit=slave_id)
        data['partial_ea'] = decode_int64(r_par.registers) if not r_par.isError() else 0.0

        # 9. Tarif (T1: 4195, T2: 4199)
        r_t1 = client.read_holding_registers(address=4195, count=4, unit=slave_id)
        data['tarif_t1'] = decode_int64(r_t1.registers) if not r_t1.isError() else 0.0

        r_t2 = client.read_holding_registers(address=4199, count=4, unit=slave_id)
        data['tarif_t2'] = decode_int64(r_t2.registers) if not r_t2.isError() else 0.0

        data['op_time'] = "Running"
        return data

    except Exception as e:
        # print(f"Error reading ID {slave_id}: {e}")
        return None

# ==========================================
# WORKER THREAD (BACKGROUND ENGINE)
# ==========================================
def worker_engine():
    global LATEST_DATA
    print("🚀 [ENGINE] Worker Thread Started (Modbus RTU Over TCP)...")
    
    # 1. Koneksi Modbus (Pakai ModbusRtuFramer sesuai hasil scan)
    modbus_client = ModbusTcpClient(IP_USR, port=PORT, framer=ModbusRtuFramer, timeout=3)
    
    # 2. Koneksi MQTT (Support Paho v2)
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Raspi_Worker_{time.time()}")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start() # Jalankan loop di background
        print("✅ [MQTT] Connected to Broker")
    except Exception as e:
        print(f"⚠️ [MQTT] Connect Fail: {e}")
        mqtt_client = None

    while True:
        # Reconnect Modbus jika putus
        if not modbus_client.connect():
            print("⚠️ [MODBUS] Reconnecting to Gateway...")
            time.sleep(5)
            continue

        # Loop Scan ID 1-18
        for mid in range(1, 19):
            # A. BACA DATA
            data = get_full_data(modbus_client, mid)
            
            if data:
                # B. SIMPAN KE MEMORI
                LATEST_DATA[mid] = data
                
                # C. KIRIM MQTT (Realtime)
                if mqtt_client:
                    try:
                        topic = f"{MQTT_TOPIC_BASE}/{mid}"
                        mqtt_client.publish(topic, json.dumps(data))
                    except:
                        pass
            
            # Jeda antar mesin agar gateway tidak tersedak (500ms)
            time.sleep(0.5)

# ==========================================
# MAIN THREAD (SUPABASE LOGGER)
# ==========================================
def main():
    # 1. Jalankan Worker
    t = threading.Thread(target=worker_engine)
    t.daemon = True
    t.start()

    # 2. Init Supabase
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except:
        print("❌ Gagal Init Supabase")
        return

    # Grouping untuk logging bertahap
    GROUPS = {
        "GROUP 1": list(range(1, 7)),
        "GROUP 2": list(range(7, 13)),
        "GROUP 3": list(range(13, 19))
    }

    print("⏳ Menunggu Worker mengumpulkan data (10 detik)...")
    time.sleep(10)

    while True:
        print(f"\n==========================================================")
        print(f"   SMART MONITORING SYSTEM - {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")

        for group_name, ids in GROUPS.items():
            print(f"\n💾 [SUPABASE] Processing {group_name}...")
            print("-" * 65)
            print(f"{'ID':<4} | {'VOLT (V)':<10} | {'ENERGY (kWh)':<15} | {'STATUS'}")
            print("-" * 65)

            for mid in ids:
                if mid in LATEST_DATA:
                    d = LATEST_DATA[mid]
                    
                    # Siapkan Payload
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
                    
                    # Kirim ke DB
                    try:
                        supabase.table("energi_db").insert(payload).execute()
                        status = "✅ SAVED"
                        volt_val = f"{d['voltage']:.1f}"
                        ea_val = f"{d['total_ea']:.1f}"
                    except Exception as e:
                        status = "❌ DB ERR"
                        volt_val = f"{d['voltage']:.1f}"
                        ea_val = "ERR"
                else:
                    status = "⚠️  NO DATA"
                    volt_val = "-"
                    ea_val = "-"
                
                print(f"{mid:<4} | {volt_val:<10} | {ea_val:<15} | {status}")

            # Jeda antar grup (Misal 5 menit per grup, atau sesuaikan)
            # Total siklus 1-18 selesai dalam 15 menit jika sleep(300)
            print(f"⏳ Selesai {group_name}. Tidur 5 Menit...")
            time.sleep(300) 

if __name__ == "__main__":
    main()
