from supabase import create_client, Client
import paho.mqtt.client as mqtt
import time
import json
import threading

# --- KONFIGURASI SUPABASE ---
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# --- KONFIGURASI MQTT ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_BASE = "monitoring/data/powermeter/#" # Dengerin semua ID

LATEST_DATA = {}
data_lock = threading.Lock()

# --- CALLBACK MQTT ---
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        mid = payload.get('meter_id')
        if mid:
            with data_lock:
                LATEST_DATA[mid] = payload
    except:
        pass

# --- MAIN ENGINE SUPABASE ---
def main():
    print("🚀 [SUPABASE ENGINE] Mulai merekam Tarif & Energi...")
    
    # 1. Init Supabase
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Supabase Terhubung!")
    except Exception as e:
        print(f"❌ Gagal Init Supabase: {e}")
        return

    # 2. Init MQTT Subscriber
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"Subscriber_{time.time()}")
    mqtt_client.on_message = on_message
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.subscribe(MQTT_TOPIC_BASE)
        mqtt_client.loop_start()
        print("✅ MQTT Listening...")
    except Exception as e:
        print(f"❌ MQTT Gagal: {e}")

    GROUPS = {
        "GROUP 1": list(range(1, 7)),
        "GROUP 2": list(range(7, 13)),
        "GROUP 3": list(range(13, 19))
    }

    # Tunggu ngumpulin data MQTT dulu 20 detik
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
                        if current_time - LATEST_DATA[mid].get('last_update', current_time) > 120:
                            del LATEST_DATA[mid] 
                        else:
                            d = LATEST_DATA[mid].copy() 
                
                if d:
                    # PAYLOAD KETAT KHUSUS SUPABASE (SESUAI TABEL LU)
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
                    print(f"{mid:<4} | {'-':<8} | {'-':<10} | {'-':<10} | {'-':<10} | ⚠️ NO MQTT DATA")

            print(f"⏳ Selesai {group_name}. Tidur 5 Menit...")
            time.sleep(300)

if __name__ == "__main__":
    main()
