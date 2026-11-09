import inspect
from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType
import time

print("==================================================")
print("🔍 INVESTIGASI PYMODBUS VERSI TERBARU")
print("==================================================")

try:
    # Menggunakan modul 'inspect' bawaan Python untuk membaca "buku panduan" asli dari fungsinya
    sig = inspect.signature(ModbusTcpClient.read_holding_registers)
    print("💡 HASIL SCAN FUNGSI 'read_holding_registers':")
    print(f"Format asli: read_holding_registers{sig}\n")
    
    print("📋 DAFTAR KATA KUNCI (PARAMETER) YANG DIIZINKAN:")
    for name, param in sig.parameters.items():
        print(f" 👉 {name}")
        
except Exception as e:
    print(f"❌ Error saat scan: {e}")

print("\n==================================================")
print("🔌 TES KONEKSI NGEBLANK (TANPA KATA KUNCI SPESIFIK)")
print("==================================================")

client = ModbusTcpClient('192.168.7.8', port=26, framer=FramerType.RTU, timeout=3)
if client.connect():
    print("✅ Koneksi ke alat SUKSES!")
    
    # Kita coba kirim tanpa nyebut 'unit' atau 'slave', siapa tau dia default ke ID 1
    print("▶️ TES: Membaca murni tanpa nyebut ID tujuan...")
    try:
        # Pymodbus baru kadang cuma butuh address dan count
        res = client.read_holding_registers(address=3019, count=2)
        if hasattr(res, 'isError') and res.isError():
            print(f"   ❌ Alat menolak (Error: {res})")
        else:
            print(f"   ✅ BERHASIL! Data mentah: {res.registers}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        
    client.close()
else:
    print("❌ GAGAL KONEK KE GATEWAY!")

print("\n==================================================")
print("SELESAI. Silakan copy semua tulisan di atas Bro!")