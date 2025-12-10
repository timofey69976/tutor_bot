"""
Главный файл для Render - HTTP сервер + Telegram бот
"""

import os
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command

PORT = int(os.getenv('PORT', 10000))
TOKEN = os.getenv('TOKEN', '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI')

# ============================================================================
# HTTP HANDLERS ДЛЯ RENDER
# ============================================================================

async def health_handler(request):
    """Health check"""
    return web.json_response({"status": "ok", "service": "tutor_bot"})

async def root_handler(request):
    """Root endpoint"""
    return web.Response(text="🤖 Telegram бот работает!", status=200)

# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

async def start_app():
    """Запускаем HTTP сервер и бота"""
    
    print("\n" + "=" * 60)
    print("🚀 ЗАПУСК ПРИЛОЖЕНИЯ")
    print("=" * 60)
    
    # Создаем HTTP приложение
    app = web.Application()
    app.router.add_get('/', root_handler)
    app.router.add_get('/health', health_handler)
    
    # Запускаем HTTP сервер
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    print(f"✅ HTTP сервер запущен на http://0.0.0.0:{PORT}")
    print("=" * 60)
    print("🤖 Telegram бот начинает работу...")
    print("=" * 60 + "\n")
    
    # Инициализируем бота
    bot = Bot(token=TOKEN)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Регистрируем минимальный обработчик
    @dp.message.register(Command("start"))
    async def start_handler(message: types.Message):
        await message.answer("👋 Бот работает! Напишите /help для справки")
    
    @dp.message.register(Command("help"))
    asyn
