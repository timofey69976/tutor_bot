"""
Telegram bot для Render - полностью независимый (без импортов из других файлов)
"""

import os
import asyncio
import sys
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
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

async def start_app():
    """Запускаем HTTP сервер и Telegram бота"""
    
    print("\n" + "=" * 70)
    print("🚀 ЗАПУСК ПРИЛОЖЕНИЯ - Telegram Bot на Render")
    print("=" * 70)
    
    # ========== HTTP СЕРВЕР ==========
    try:
        print("⏳ Создание HTTP приложения...")
        app = web.Application()
        app.router.add_get('/', root_handler)
        app.router.add_get('/health', health_handler)
        print("✅ HTTP приложение создано")
        
        print("⏳ Запуск HTTP сервера на 0.0.0.0:{}...".format(PORT))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.
