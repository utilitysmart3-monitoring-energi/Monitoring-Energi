from pymodbus.client import ModbusTcpClient
from supabase import create_client, Client
import struct
import time
import os

# --- KONFIGURASI MODBUS ---
IP_USR = '192.168.5.20'
PORT = 502

# --- KONFIGURASI SUPABASE ---
SUPABASE_URL = "https://awyohlizbltikpfanugb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF3eW9obGl6Ymx0aWtwZmFudWdiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAwNzE0MjMsImV4cCI6MjA4NTY0NzQyM30.t2WyNo8Th4I-8aMZIRdCT9icVGJrKdT9oCtN04IRTes"

# Inisialisasi Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- PEMBAGIAN KELOMPOK ID ---
GROUP_1 = list(range(1, 7))   # ID 1 - 6
GROUP_2 = list(range(7, 13))  # ID 7 - 12
GROUP_3 = list(range(13, 19)) # ID 13 - 18

def decode_int64(registers):
    # Gabung 4 register jadi INT64, bagi 1000 buat ke kWh
    packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
    val_int = struct.unpack('>Q', packed)[0]
    return val_int / 1000.0

def get_energy_data(client, slave_id):
    try:
        # Baca 4 parameter utama (Total, Partial, T1, T2)
        # Format INT64 (4 Register)
        
        # 1. Total Ea (Reg 3204 -> Addr 3203)
        r1 = client.read_holding_registers(address=3203, count=4, device_id=slave_id)
        val_total = decode_int64(r1.registers) if not r1.isError() else None

        # 2. Partial Ea (Reg 3256 -> Addr 3255)
        r2 = client.read_holding_registers(address=3255, count=4, device_id=slave_id)
        val_partial = decode_int64(r2.registers) if not r2.isError() else None

        # 3. Tarif T1 (Reg 4196 -> Addr 4195)
        r3 = client.read_holding_registers(address=4195, count=4, device_id=slave_id)
        val_t1 = decode_int64(r3.registers) if not r3.isError() else None

        # 4. Tarif T2 (Reg 4200 -> Addr 4199)
        r4 = client.read_holding_registers(address=4199, count=4, device_id=slave_id)
        val_t2 = decode_int64(r4.registers) if not r4.isError() else None

        return (val_total, val_partial, val_t1, val_t2)
    except:
        return (None, None, None, None)

def process_group(client, group_ids, group_name):
    print(f"\n🚀 MEMPROSES {group_name} (ID {group_ids[0]}-{group_ids[-1]})")
    print("-" * 60)
    print(f"{'ID':<4} | {'TOTAL (kWh)':<12} | {'STATUS KIRIM':<15}")
    print("-" * 60)

    for slave_id in group_ids:
        # 1. Ambil Data dari Meteran
        total, partial, t1, t2 = get_energy_data(client, slave_id)

        status_kirim = "❌ SKIP (Err)"
        
        if total is not None:
            # 2. Siapkan Payload Data
            data_payload = {
                "meter_id": slave_id,
                "total_ea": total,
                "partial_ea": partial,
                "tarif_t1": t1,
                "tarif_t2": t2
            }

            # 3. Kirim ke Supabase
            try:
                response = supabase.table("energi_db").insert(data_payload).execute()
                status_kirim = "✅ TERKIRIM"
            except Exception as e:
                status_kirim = f"❌ GAGAL API: {e}"
            
            print(f"{slave_id:<4} | {total:>12.2f} | {status_kirim}")
        else:
            print(f"{slave_id:<4} | {'ERROR':>12} | {status_kirim}")

def main():
    client = ModbusTcpClient(IP_USR, port=PORT)
    
    while True:
        os.system('clear')
        print(f"==========================================================")
        print(f"   MONITORING & UPLOAD SUPABASE - {time.strftime('%H:%M:%S')}")
        print(f"==========================================================")

        if not client.connect():
            print("❌ Gagal konek ke USR (Modbus Gateway). Retrying 10s...")
            time.sleep(10)
            continue

        # --- FASE 1: ID 1-6 (5 Menit Pertama) ---
        process_group(client, GROUP_1, "KELOMPOK 1")
        print("\n⏳ Selesai Kelompok 1. Menunggu 5 Menit untuk Kelompok 2...")
        time.sleep(300) # Tidur 300 detik (5 Menit)

        # --- FASE 2: ID 7-12 (5 Menit Kedua) ---
        process_group(client, GROUP_2, "KELOMPOK 2")
        print("\n⏳ Selesai Kelompok 2. Menunggu 5 Menit untuk Kelompok 3...")
        time.sleep(300) # Tidur 300 detik (5 Menit)

        # --- FASE 3: ID 13-18 (5 Menit Terakhir) ---
        process_group(client, GROUP_3, "KELOMPOK 3")
        print("\n⏳ Selesai Kelompok 3. Menunggu 5 Menit (Siklus Selesai)...")
        time.sleep(300) # Tidur 300 detik (5 Menit)

        # --- DELAY TAMBAHAN 1 MENIT ---
        print("\n☕ Istirahat 1 Menit sebelum siklus baru...")
        time.sleep(60)

        # Setelah ini loop akan kembali ke atas (Clear screen -> Kelompok 1 lagi)

if __name__ == "__main__":
    main()
