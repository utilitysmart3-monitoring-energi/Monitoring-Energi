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
GEMINI_API_KEY = "AIzaSyBdh38__ayg6Kz1lUrP5TAz8kHi2UabUWA"

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

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

        print(f"DEBUG: Menerima data ID {data.get('meter_id', 'Unknown')}. Menghubungi Google...")
        response = model.generate_content(prompt)
        
        return jsonify({
            "status": "success",
            "result": response.text
        })

    except Exception as e:
        # 🔥 MODE DEBUG: Kirim error aslinya ke web HTML
        error_detail = str(e)
        print(f"❌ ERROR DARI GOOGLE: {error_detail}")
        return jsonify({
            "status": "error", 
            "result": f"Google AI Error: {error_detail}" 
        }), 500

if __name__ == '__main__':
    print("==========================================================")
    print("🤖 AI ENGINE DEBUG MODE - PORT 5000 🤖")
    print("==========================================================")
    app.run(host='0.0.0.0', port=5000)
