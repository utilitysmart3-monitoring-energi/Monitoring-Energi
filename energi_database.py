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
    days = raw_seconds // 86400            
    hours = (raw_seconds % 86400) // 3600  
    return f"{days}d {hours}h"

# --- FUNGSI BACA DATA (WORKER SCADA) ---
def get_full_data(client, slave_id):
    try:
        data = {'meter_id': slave_id}
        DELAY_RS485 = 0.05  # Micro-delay 50ms untuk nafas Gateway USR
        
        # 5. Voltage (3020 -> 3019) PING DENGAN RETRY
        r = None
        for attempt in range(2):
            r = client.read_holding_registers(address=3019, count=2, unit=slave_id)
            if not r.isError():
                break
            time.sleep(0.3) 
            
        if not r.isError():
            data['voltage'] = decode_float(r.registers)
        else:
            return None # Benar-benar mati/offline

        time.sleep(DELAY_RS485)

        # 6. Current I1, I2, I3 (3000 -> 2999, Length 6)
        r = client.read_holding_registers(address=2999, count=6, unit=slave_id)
        if not r.isError():
            data['current_i1'] = decode_float(r.registers[0:2])
            data['current_i2'] = decode_float(r.registers[2:4])
            data['current_i3'] = decode_float(r.registers[4:6])
        else:
            data['current_i1'] = data['current_i2'] = data['current_i3'] = 0.0

        time.sleep(DELAY_RS485)

        # 7. Power P / Active (3054 -> 3053)
        r_p = client.read_holding_registers(address=3053, count=2, unit=slave_id)
        data['active_p'] = decode_float(r_p.registers) if not r_p.isError() else 0.0
        
        time.sleep(DELAY_RS485)

        # 8. Power Q / Reactive (3060 -> 3059)
        r_q = client.read_holding_registers(address=3059, count=2, unit=slave_id)
        data['reactive_q'] = decode_float(r_q.registers) if not r_q.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 9. Power S / Apparent (3068 -> 3067)
        r_s = client.read_holding_registers(address=3067, count=2, unit=slave_id)
        data['apparent_s'] = decode_float(r_s.registers) if not r_s.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 10. PF (3084 -> 3083)
        r_pf = client.read_holding_registers(address=3083, count=2, unit=slave_id)
        data['pf'] = decode_float(r_pf.registers) if not r_pf.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 11. Freq (3110 -> 3109)
        r_fr = client.read_holding_registers(address=3109, count=2, unit=slave_id)
        data['freq'] = decode_float(r_fr.registers) if not r_fr.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 1. Total EA (3204 -> 3203)
        r_tot = client.read_holding_registers(address=3203, count=4, unit=slave_id)
        data['total_ea'] = decode_int64(r_tot.registers) if not r_tot.isError() else 0.0
        
        time.sleep(DELAY_RS485)

        # 3. Partial EA (3256 -> 3255)
        r_par = client.read_holding_registers(address=3255, count=4, unit=slave_id)
        data['partial_ea'] = decode_int64(r_par.registers) if not r_par.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 13. Tarif T1 (4196 -> 4195)
        r_t1 = client.read_holding_registers(address=4195, count=4, unit=slave_id)
        data['tarif_t1'] = decode_int64(r_t1.registers) if not r_t1.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 14. Tarif T2 (4200 -> 4199)
        r_t2 = client.read_holding_registers(address=4199, count=4, unit=slave_id)
        data['tarif_t2'] = decode_int64(r_t2.registers) if not r_t2.isError() else 0.0
        
        time.sleep(DELAY_RS485)

        # 2. Total ER (3220 -> 3219)
        r_tot_er = client.read_holding_registers(address=3219, count=4, unit=slave_id)
        data['total_er'] = decode_int64(r_tot_er.registers) if not r_tot_er.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 4. Partial ER (3272 -> 3271)
        r_par_er = client.read_holding_registers(address=3271, count=4, unit=slave_id)
        data['partial_er'] = decode_int64(r_par_er.registers) if not r_par_er.isError() else 0.0

        time.sleep(DELAY_RS485)

        # 12. Op Time (2004 -> 2003)
        r_op = client.read_holding_registers(address=2003, count=2, unit=slave_id)
        data['op_time'] = decode_int32(r_op.registers) if not r_op.isError() else 0

        # Timestamp Watchdog
        data['last_update'] = time.time()

        return data

    except Exception as e:
        return None

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
            
            time.sleep(1)

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
            # Tampilan Terminal disesuaikan dengan isi Database biar nyambung
            print(f"{'ID':<4} | {'VOLT (V)':<8} | {'PART EA(kWh)':<12} | {'TARIF T1':<10} | {'TARIF T2':<10} | {'STATUS'}")
            print("-" * 85)

            for mid in ids:
                d = None
                with data_lock:
                    if mid in LATEST_DATA:
                        if current_time - LATEST_DATA[mid].get('last_update', current_time) > 120:
                            del LATEST_DATA[mid] 
                        else:
                            d = LATEST_DATA[mid].copy() 
                
                if d:
                    # ⚠️ FILTER PAYLOAD: HANYA MEMASUKKAN KOLOM YANG ADA DI SKEMA DATABASE LU
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
