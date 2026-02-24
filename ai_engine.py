from flask import Flask, request, jsonify
from flask_cors import CORS
import warnings
import google.generativeai as genai
import sys

# 🔥 PEREDAM WARNING
warnings.filterwarnings("ignore")

# =========================================================
# 🔧 KONFIGURASI API GEMINI (PENTING: PASTIKAN KEY BENAR)
# =========================================================
# Pastikan TIDAK ADA SPASI di awal atau akhir string ini.
GEMINI_API_KEY = "AIzaSyCfhpGRDp5maiJKleH2j6ciOM6Jbd1HZVg"

genai.configure(api_key=GEMINI_API_KEY)

# 🔍 SCAN MODEL YANG TERSEDIA SECARA DINAMIS
available_models = []

def refresh_model_list():
    global available_models
    print("\n🔎 SEDANG MENCARI MODEL YANG DIIZINKAN GOOGLE...")
    print("-" * 50)
    discovered = []
    try:
        # Mencoba list model yang benar-benar bisa dipakai
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                # Kita simpan nama pendeknya juga (tanpa 'models/')
                short_name = m.name.replace('models/', '')
                discovered.append(m.name)
                discovered.append(short_name)
                print(f"✅ AKTIF: {m.name}")
        
        available_models = list(set(discovered)) # Hapus duplikat
        if not available_models:
            print("❌ PERINGATAN: Tidak ada model ditemukan! Pastikan API Key baru sudah benar.")
    except Exception as e:
        error_str = str(e)
        if "API_KEY_INVALID" in error_str:
            print("❌ ERROR FATAL: API KEY TIDAK VALID / SALAH KETIK!")
        else:
            print(f"❌ GAGAL SCAN MODEL: {error_str}")
    print("-" * 50)

# Jalankan scan saat startup
refresh_model_list()

app = Flask(__name__)
CORS(app)

@app.route('/analyze', methods=['POST'])
def analyze_data():
    try:
        data = request.json
        prompt = f"""
        Anda adalah Senior Electrical Engineer. Analisa data berikut:
        Voltage: {data.get('voltage', 0)} V
        Current: I1={data.get('i1', 0)}A, I2={data.get('i2', 0)}A, I3={data.get('i3', 0)}A
        PF: {data.get('pf', 0)}
        P: {data.get('p', 0)} Watt
        Berikan: Status Beban, Analisa Arus, dan Rekomendasi Teknis Singkat.
        Jawab dalam Bahasa Indonesia.
        """

        # Jika list kosong, coba scan ulang
        if not available_models:
            refresh_model_list()

        response_text = None
        last_err = ""
        
        # Urutan prioritas cadangan
        priority = ['gemini-1.5-flash', 'gemini-pro', 'models/gemini-1.5-flash', 'models/gemini-pro']
        
        # Gabungkan list hasil scan dengan list priority
        test_queue = []
        for p in priority:
            if p not in test_queue: test_queue.append(p)
        for a in available_models:
            if a not in test_queue: test_queue.append(a)

        print(f"🚀 Memulai pencarian model untuk ID: {data.get('meter_id', 'Unknown')}")

        for model_id in test_queue:
            try:
                print(f"⏳ Mencoba: {model_id}...")
                model = genai.GenerativeModel(model_id)
                response = model.generate_content(prompt)
                
                if response and response.text:
                    response_text = response.text
                    print(f"✅ BERHASIL MENGGUNAKAN: {model_id}")
                    break 
            except Exception as e:
                last_err = str(e)
                print(f"❌ Gagal ({model_id}): {last_err[:60]}...")
                continue

        if response_text:
            return jsonify({"status": "success", "result": response_text})
        else:
            # 🔥 PESAN ERROR KHUSUS JIKA API KEY INVALID
            if "API_KEY_INVALID" in last_err:
                pesan_error = "API Key Google Anda ditolak (INVALID). Silakan pastikan tulisan API Key di file ai_engine.py sudah benar, tidak ada spasi yang terikut, atau buat kunci baru di Google AI Studio."
            else:
                pesan_error = f"Semua model gagal. Error terakhir: {last_err}"
                
            return jsonify({
                "status": "error", 
                "result": pesan_error
            }), 500

    except Exception as e:
        return jsonify({"status": "error", "result": str(e)}), 500

if __name__ == '__main__':
    print("\n==========================================================")
    print("🤖 AI ENGINE ULTIMATE RECOVERY - PORT 5000 🤖")
    print(f"Python Version: {sys.version.split()[0]}")
    print("==========================================================\n")
    app.run(host='0.0.0.0', port=5000)
