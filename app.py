"""
HTTP сервер для Render + Telegram бот
Работает на одном процессе с async/await
"""

import os
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import uuid

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

TOKEN = os.getenv('TOKEN', '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI')
TUTOR_ID = int(os.getenv('TUTOR_ID', '1339816111'))
PORT = int(os.getenv('PORT', 10000))

SUBJECTS = ["Математика", "Физика", "Химия"]

# ============================================================================
# HTTP HANDLERS ДЛЯ RENDER
# ============================================================================

async def health_handler(request):
    """Health check - для Render"""
    return web.json_response({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "service": "tutor_bot"
    })

async def root_handler(request):
    """Root endpoint"""
    return web.Response(text="Bot is running! 🤖", status=200)

# ============================================================================
# ЗАПУСК БОТ + HTTP СЕРВЕР
# ============================================================================

async def main():
    """Запускаем бота и HTTP сервер одновременно"""
    
    # Инициализируем бота и диспетчер
    bot = Bot(token=TOKEN)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Регистрируем обработчики (минимальный набор)
    @dp.message.register(Command("start"))
    async def start_handler(message: types.Message):
        await message.answer("👋 Добро пожаловать!")
    
    # Создаем HTTP приложение для Render
    app = web.Application()
    app.router.add_get('/', root_handler)
    app.router.add_get('/health', health_handler)
    
    # Запускаем HTTP сервер
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    print("=" * 60)
    print(f"✅ HTTP сервер запущен на http://0.0.0.0:{PORT}")
    print("=" * 60)
    print("🤖 Telegram бот запускается...")
    print("=" * 60)
    
    try:
        # Запускаем бота в фоновой задаче
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        print(f"❌ Ошибка бота: {e}")
    finally:
        await runner.cleanup()
        await bot.session.close()

if __name__ == "__main__":
    print("\n🚀 Запуск приложения...\n")
    asyncio.run(main())
