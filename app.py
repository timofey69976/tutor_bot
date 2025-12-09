from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running! 🤖", 200

@app.route('/health')
def health():
    return {"status": "ok"}, 200

if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    print(f"🌐 HTTP сервер запущен на http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
