"""
Telegram bot для Render - полностью независимый (без импортов из других файлов)
"""

import os
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

PORT = int(os.getenv('PORT', 10000))
TOKEN = os.getenv('TOKEN')

if not TOKEN:
    print("❌ ОШИБКА: TOKEN не установлен в переменных окружения!")
    print("Добавьте TOKEN в Render Settings → Environment Variables")
    exit(1)

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
    print(f"📌 Порт: {PORT}")
    print(f"🔑 Токен загружен: {'✅ Да' if TOKEN else '❌ Нет'}")
    print("=" * 70)
    
    # ========== HTTP СЕРВЕР ==========
    print("\n⏳ Запуск HTTP сервера...")
    app = web.Application()
    app.router.add_get('/', root_handler)
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    print(f"✅ HTTP сервер запущен на 0.0.0.0:{PORT}")
    print(f"   Health check: http://0.0.0.0:{PORT}/health")
    
    # ========== TELEGRAM БОТ =====
