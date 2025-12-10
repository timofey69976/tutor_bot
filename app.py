"""
Telegram bot для Render - HTTP сервер + Бот в отдельных потоках
"""

import os
import asyncio
import sys
import threading
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command

# Получаем переменные окружения
PORT = int(os.getenv('PORT', 10000))
TOKEN = os.getenv('TOKEN')

print("=" * 70)
print("🚀 ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ")
print("=" * 70)
print(f"📌 Порт: {PORT}")
print(f"🔑 Токен: {'✅ Установлен' if TOKEN else '⚠️  НЕ установлен (используется default)'}")
print("=" * 70)

if not TOKEN:
    TOKEN = '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI'
    print("⚠️  Используется default TOKEN\n")

sys.stdout.flush()

async def health_handler(request):
    return web.json_response({"status": "ok", "service": "tutor_bot"})

async def root_handler(request):
    return web.Response(text="🤖 Telegram бот работает!", status=200)

async def run_http_server():
    try:
        print("⏳ Создание HTTP приложения...")
        app = web.Application()
        app.router.add_get('/', root_handler)
        app.router.add_get('/health', health_handler)
        print("✅ HTTP приложение создано")
        
        print(f"⏳ Запуск HTTP сервера на 0.0.0.0:{PORT}...")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        
        print(f"✅ HTTP сервер запущен на 0.0.0.0:{PORT}")
        print("=" * 70)
        print("🤖 БОТ ГОТОВ К РАБОТЕ")
        print("=" * 70)
        sys.stdout.flush()
        
        await asyncio.sleep(float('inf'))
    except Exception as e:
        print(f"❌ Ошибка HTTP сервера: {e}")
        import traceback
        traceback.print_exc()

def run_bot():
    try:
        print("\n⏳ Инициализация Telegram бота...")
        
        bot_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(bot_loop)
        
        async def bot_main():
            try:
                print("⏳ Создание бота...")
                bot = Bot(token=TOKEN)
                storage = MemoryStorage()
                dp = Dispatcher(storage=storage)
                print("✅ Диспетчер создан")
                
                @dp.message.register(Command("start"))
                async def start_handler(message: types.Message):
                    await message.answer("👋 Привет! Я бот для управления расписанием репетитора.\n\nДоступные команды:\n/help - справка\n/status - статус бота")
                
                @dp.message.register(Command("help"))
                async def help_handler(message: types.Message):
                    await message.answer("📖 Справка:\n/start - начать\n/help - эта справка\n/status - проверить статус")
                
                @dp.message.register(Command("status"))
                async def status_handler(message: types.Message):
                    await message.answer("✅ Бот работает нормально!")
                
                @dp.message.register()
                async def echo_handler(message: types.Message):
                    await message.answer(f"Вы написали: {message.text}\n\nНапишите /help для справки")
                
                print("✅ Обработчики зарегистрированы")
                print("⏳ Ожидание сообщений...\n")
                sys.stdout.flush()
                
                await dp.start_polling(bot, skip_updates=True)
                
            except Exception as e:
                print(f"❌ Ошибка бота: {e}")
                import traceback
                traceback.print_exc()
        
        bot_loop.run_until_complete(bot_main())
        
    except Exception as e:
        print(f"❌ Критическая ошибка бота: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        print("⏳ Запуск HTTP сервера в главном потоке...\n")
        sys.stdout.flush()
        
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        
        asyncio.run(run_http_server())
        
    except KeyboardInterrupt:
        print("\n⏹️  Приложение остановлено")
    except Exception as e:
        print(f"❌ Ошибка главного потока: {e}")
        import traceback
        traceback.print_exc()
