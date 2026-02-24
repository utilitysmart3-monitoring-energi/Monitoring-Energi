from flask import Flask, request, jsonify
from flask_cors import CORS
import warnings
import google.generativeai as genai

# 🔥 PEREDAM WARNING
warnings.filterwarnings("ignore")

# =========================================================
# 🔧 KONFIGURASI API GEMINI
# =========================================================
GEMINI_API_KEY = "AIzaSyBdh38__ayg6Kz1lUrP5TAz8kHi2UabUWA"
genai.configure(api_key=GEMINI_API_KEY)

# 🔍 SCAN MODEL YANG TERSEDIA
print("🔎 Scanning model AI yang tersedia untuk API Key Anda...")
available_models = []
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            available_models.append(m.name)
            print(f"✅ Ditemukan: {m.name}")
except Exception as e:
    print(f"❌ Gagal scan model: {e}")

# Tentukan urutan prioritas model
PRIORITY_MODELS = [
    'models/gemini-1.5-flash',
    'models/gemini-1.5-flash-latest',
    'models/gemini-pro',
    'models/gemini-1.0-pro'
]

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
        Berikan: Status Beban, Analisa Unbalance, dan Rekomendasi.
        """

        # 🔥 TRY LOOP: Coba setiap model sampai ada yang berhasil
        response_text = None
        last_err = ""
        
        for model_id in PRIORITY_MODELS:
            try:
                print(f"⏳ Mencoba model: {model_id}...")
                model = genai.GenerativeModel(model_id)
                response = model.generate_content(prompt)
                response_text = response.text
                print(f"🚀 BERHASIL pake model: {model_id}")
                break # Berhenti kalau sudah sukses
            except Exception as e:
                last_err = str(e)
                print(f"⚠️ Model {model_id} gagal: {last_err}")
                continue

        if response_text:
            return jsonify({"status": "success", "result": response_text})
        else:
            return jsonify({"status": "error", "result": f"Semua model gagal. Error terakhir: {last_err}"}), 500

    except Exception as e:
        return jsonify({"status": "error", "result": str(e)}), 500

if __name__ == '__main__':
    print("==========================================================")
    print("🤖 AI ENGINE SELF-HEALING MODE - PORT 5000 🤖")
    print("==========================================================")
    app.run(host='0.0.0.0', port=5000)
