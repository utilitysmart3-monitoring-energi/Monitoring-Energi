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

LATEST_DATA = {}

# --- DECODER FUNCTIONS ---
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
    """Konversi detik untuk ditampilkan di Terminal"""
    days = raw_seconds // 86400            
    hours = (raw_seconds % 86400) // 3600  
    return f"{days}d {hours}h"

# --- FUNGSI BACA DATA (WORKER) ---
def get_full_data(client, slave_id):
    try:
        data = {'meter_id': slave_id}
        
        # 1. Voltage (3019)
        r = client.read_holding_registers(address=3019, count=2, unit=slave_id)
        if not r.isError():
            data['voltage'] = decode_float(r.registers)
        else:
            return None 

        # 2. Current (2999)
        r = client.read_holding_registers(address=2999, count=6, unit=slave_id)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

        # 3. Tarif (T1: 4195, T2: 4199)
        r_t1 = client.read_holding_registers(address=4195, count=4, unit=slave_id)
        data['tarif_t1'] = decode_int64(r_t1.registers) if not r_t1.isError() else 0.0

        r_t2 = client.read_holding_registers(address=4199, count=4, unit=slave_id)
        data['tarif_t2'] = decode_int64(r_t2.registers) if not r_t2.isError() else 0.0

        # 4. Energy Total EA (3203) & Partial EA (3255)
        r_tot = client.read_holding_registers(address=3203, count=4, unit=slave_id)
        data['total_ea'] = decode_int64(r_tot.registers) if not r_tot.isError() else 0.0
        
        r_par = client.read_holding_registers(address=3255, count=4, unit=slave_id)
        data['partial_ea'] = decode_int64(r_par.registers) if not r_par.isError() else 0.0

        # --- 3 PARAMETER BARU ---
        
        # 5. Total ER (3219, Panjang 4)
        r_tot_er = client.read_holding_registers(address=3219, count=4, unit=slave_id)
        data['total_er'] = decode_int64(r_tot_er.registers) if not r_tot_er.isError() else 0.0

        # 6. Partial ER (3271, Panjang 4)
        r_par_er = client.read_holding_registers(address=3271, count=4, unit=slave_id)
        data['partial_er'] = decode_int64(r_par_er.registers) if not r_par_er.isError() else 0.0

        # 7. Operating Time (2003, Panjang 2, Data Mentah Detik) - Schneider
        r_op = client.read_holding_registers(address=2003, count=2, unit=slave_id)
        data['op_time'] = decode_int32(r_op.registers) if not r_op.isError() else 0

        return data
    except:
        return None

# ==========================================
# WORKER THREAD (BACKGROUND ENGINE)
# ==========================================
def worker_engine():
    global LATEST_DATA
    modbus_client = ModbusTcpClient(IP_USR, port=PORT, framer=ModbusRtuFramer, timeout=3)
    
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Raspi_Worker_{time.time()}")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start() 
    except:
        mqtt_client = None

    while True:
        if not modbus_client.connect():
            time.sleep(5)
            continue

        for mid in range(1, 19):
            data = get_full_data(modbus_client, mid)
            if data:
                LATEST_DATA[mid] = data
                if mqtt_client:
                    try:
                        topic = f"{MQTT_TOPIC_BASE}/{mid}"
                        mqtt_client.publish(topic, json.dumps(data))
                    except:
                        pass
            time.sleep(0.5)

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
        return

    GROUPS = {
        "GROUP 1": list(range(1, 7)),
        "GROUP 2": list(range(7, 13)),
        "GROUP 3": list(range(13, 19))
    }

    time.sleep(10)

    while True:
        print(f"\n==========================================================")
        print(f"   SMART MONITORING SYSTEM - {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")

        for group_name, ids in GROUPS.items():
            print(f"\n💾 [SUPABASE] Processing {group_name}...")
            print("-" * 75)
            print(f"{'ID':<4} | {'VOLT (V)':<8} | {'TOT ER (kWh)':<12} | {'OP TIME':<12} | {'STATUS'}")
            print("-" * 75)

            for mid in ids:
                if mid in LATEST_DATA:
                    d = LATEST_DATA[mid]
                    
                    # Payload siap kirim ke DB (Op Time disimpan murni dalam Detik)
                    payload = {
                        "meter_id": d['meter_id'],
                        "voltage": d['voltage'],
                        "current_i1": d['current_i1'],
                        "current_i2": d['current_i2'],
                        "current_i3": d['current_i3'],
                        "tarif_t1": d['tarif_t1'],
                        "tarif_t2": d['tarif_t2'],
                        "partial_ea": d['partial_ea'],
                        "total_er": d['total_er'],
                        "partial_er": d['partial_er'],
                        "op_time": d['op_time']
                    }
                    
                    try:
                        supabase.table("energi_db").insert(payload).execute()
                        status = "✅ SAVED"
                        volt_val = f"{d['voltage']:.1f}"
                        er_val = f"{d['total_er']:.1f}"
                        op_str = format_dari_detik(d['op_time'])
                    except Exception as e:
                        status = "❌ DB ERR"
                        volt_val = f"{d['voltage']:.1f}"
                        er_val = "ERR"
                        op_str = "ERR"
                else:
                    status = "⚠️  NO DATA"
                    volt_val = "-"
                    er_val = "-"
                    op_str = "-"
                
                print(f"{mid:<4} | {volt_val:<8} | {er_val:<12} | {op_str:<12} | {status}")

            time.sleep(300) 

if __name__ == "__main__":
    main()
