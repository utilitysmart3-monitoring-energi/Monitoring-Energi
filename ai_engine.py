from flask import Flask, request, jsonify
from flask_cors import CORS
import warnings
import threading
import google.generativeai as genai

# 🔥 PEREDAM WARNING
warnings.filterwarnings("ignore")

# =========================================================
# 🔧 KONFIGURASI API GEMINI
# =========================================================
# Gunakan API Key yang sudah Anda buat
GEMINI_API_KEY = "AIzaSyBdh38__ayg6Kz1lUrP5TAz8kHi2UabUWA"

genai.configure(api_key=GEMINI_API_KEY)

# 🔥 FIX: Menggunakan 'gemini-1.5-flash' (Nama paling standar dan stabil)
# Jika ini masih 404, coba ganti menjadi 'gemini-pro'
MODEL_NAME = 'gemini-1.5-flash'
model = genai.GenerativeModel(MODEL_NAME)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

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
        
        Berikan laporan singkat: Status Beban, Analisa Unbalance, dan Rekomendasi Teknis.
        """

        print(f"DEBUG: Menerima request untuk ID {data.get('meter_id', 'Unknown')}. Menghubungi Google AI ({MODEL_NAME})...")
        
        response = model.generate_content(prompt)
        
        return jsonify({
            "status": "success",
            "result": response.text
        })

    except Exception as e:
        error_msg = str(e)
        print(f"❌ ERROR AI ENGINE: {error_msg}")
        return jsonify({
            "status": "error", 
            "result": f"Google AI Error: {error_msg}" 
        }), 500

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({"status": "online", "message": "Satpam AI Pi Siap!"})

if __name__ == '__main__':
    print("==========================================================")
    print(f"🤖 AI ENGINE READY - MODEL: {MODEL_NAME} - PORT 5000 🤖")
    print("==========================================================")
    app.run(host='0.0.0.0', port=5000, debug=False)
