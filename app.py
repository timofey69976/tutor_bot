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

# Используем default токен если не установлен
if not TOKEN:
    TOKEN = '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI'
    print("⚠️  Используется default TOKEN\n")

sys.stdout.flush()

# ============================================================================
# HTTP HANDLERS ДЛЯ RENDER
# ============================================================================

async def health_handler(request):
    """Health check для Render"""
    return web.json_response({"status": "ok", "service": "tutor_bot"})

async def root_handler(request):
    """Root endpoint"""
    return web.Response(text="🤖 Telegram бот работает!", status=200)

# ============================================================================
# HTTP СЕРВЕР (главный async loop)
# ============================================================================

async def run_http_server():
    """Запускаем HTTP сервер"""
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
        
