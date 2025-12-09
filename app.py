from flask import Flask
import subprocess
import threading

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running! 🤖"

def run_bot():
    subprocess.run(["python", "tutor_bot.py"])

if __name__ == '__main__':
    # Запускаем бота в отдельном потоке
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    # Запускаем Flask на порту 10000
    app.run(host='0.0.0.0', port=10000)
