from flask import Flask
import asyncio
import threading
from tutor_bot import main

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running! 🤖"

def run_bot():
    """Запускаем бота в отдельном потоке"""
    asyncio.run(main())

if __name__ == '__main__':
    # Запускаем бота в фоновом потоке
    bot_thread = threading.Thread(target=run_bot, daemon=False)
    bot_thread.start()
    
    # Запускаем Flask на порту 10000
    app.run(host='0.0.0.0', port=10000, debug=False)
