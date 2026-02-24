from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
import threading

# =========================================================
# 🔧 KONFIGURASI API GEMINI (TARUH KUNCI RAHASIA DI SINI)
# =========================================================
# Dapatkan di: https://aistudio.google.com/app/apikey
GEMINI_API_KEY = "AIzaSyBdh38__ayg6Kz1lUrP5TAz8kHi2UabUWA"

# Konfigurasi AI
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# Bikin Server API Lokal (Satpam)
app = Flask(__name__)
CORS(app) # Biar HTML lu di laptop bisa manggil server ini

@app.route('/analyze', methods=['POST'])
def analyze_data():
    try:
        # 1. Terima data dari HTML (dikirim pas tombol di klik)
        data = request.json
        
        # 2. Susun pertanyaan untuk AI (Prompt Engineering)
        prompt = f"""
        Anda adalah Senior Electrical Engineer yang ahli dalam pemeliharaan industri.
        Analisa data realtime dari Power Meter berikut secara singkat dan padat:

        Voltage: {data.get('voltage', 0)} V
        Current I1: {data.get('i1', 0)} A
        Current I2: {data.get('i2', 0)} A
        Current I3: {data.get('i3', 0)} A
        Power Factor: {data.get('pf', 0)}
        Active Power: {data.get('p', 0)} kW

        Berikan laporan dalam format ini:
        1. Status Beban: (Normal / Overload / Underload)
        2. Analisa Arus (Unbalance): (Apakah arus R S T seimbang?)
        3. Rekomendasi Teknis: (Satu kalimat aksi yang harus dilakukan teknisi)
        """

        # 3. Tanya ke Gemini
        print("Minta wangsit ke AI...")
        response = model.generate_content(prompt)
        
        # 4. Kirim balasan AI ke HTML
        return jsonify({
            "status": "success",
            "result": response.text
        })

    except Exception as e:
        print(f"Error AI: {e}")
        return jsonify({"status": "error", "result": "Gagal menghubungi AI Server."}), 500

if __name__ == '__main__':
    print("==========================================================")
    print("🤖 AI ENGINE BERJALAN DI PORT 5000 🤖")
    print("==========================================================")
    # Jalan di port 5000, bisa diakses dari laptop lu lewat WiFi
    app.run(host='0.0.0.0', port=5000)
