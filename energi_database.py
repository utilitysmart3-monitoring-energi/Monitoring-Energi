from pymodbus.client import ModbusTcpClient
import struct
import time
import os

# --- KONFIGURASI ---
IP_USR = '192.168.5.20'
PORT = 502
TARGET_IDS = list(range(1, 19)) # Scan ID 1 s/d 18

def decode_int64(registers):
    # Gabung 4 register (16-bit x 4) menjadi 1 angka besar (64-bit Integer)
    # Urutan: Big Endian (High Word First)
    packed = struct.pack('>HHHH', registers[0], registers[1], registers[2], registers[3])
    val_int = struct.unpack('>Q', packed)[0] # Q = Unsigned Long Long (64-bit)
    
    # Schneider biasanya simpan dalam Wh, kita ubah ke kWh (dibagi 1000)
    return val_int / 1000.0

def get_energy_data(client, slave_id):
    try:
        # PENTING: count=4 karena formatnya INT64
        
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

def main():
    client = ModbusTcpClient(IP_USR, port=PORT)
    
    while True:
        if not client.connect():
            print("❌ Gagal Konek USR... Retrying...")
            time.sleep(5)
            continue

        os.system('clear')
        print(f"==========================================================================")
        print(f"   AUDIT ENERGI SCHNEIDER (INT64 FORMAT) - {time.strftime('%H:%M:%S')}")
        print(f"==========================================================================")
        print(f"ID  | TOTAL Ea (kWh) | PARTIAL (kWh)| TARIF T1   | TARIF T2   ")
        print(f"----|----------------|--------------|------------|------------")

        for slave_id in TARGET_IDS:
            total, partial, t1, t2 = get_energy_data(client, slave_id)
            
            if total is not None:
                # Format print biar rapi
                print(f"{slave_id:<3} | {total:>14.2f} | {partial:>12.2f} | {t1:>10.2f} | {t2:>10.2f}")
            else:
                print(f"{slave_id:<3} |          ERROR |        ERROR |      ERROR |      ERROR")
        
        print(f"==========================================================================")
        print("Update data dalam 5 detik...")
        time.sleep(5)

if __name__ == "__main__":
    main()
