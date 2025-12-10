# -*- coding: utf-8 -*-
"""
Telegram бот для управления расписанием занятий репетитора
Адаптирован для работы на Render с HTTP сервером
С МАКСИМАЛЬНОЙ ЗАЩИТОЙ ОТ КОНФЛИКТОВ И СИСТЕМОЙ ПОДТВЕРЖДЕНИЯ
С KEEP-ALIVE ДЛЯ ПРЕДОТВРАЩЕНИЯ ГИБЕРНАЦИИ
"""

import os
import asyncio
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

PORT = int(os.getenv('PORT', 10000))
TOKEN = os.getenv('TOKEN')
RENDER_URL = os.getenv('RENDER_URL', '')  # https://your-app.onrender.com

if not TOKEN:
    TOKEN = '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI'

TUTOR_ID = 1339816111
SUBJECTS = ["Математика", "Физика", "Химия"]
DEFAULT_SCHEDULE = {
    "Monday": [f"{h}:00" for h in range(18, 21)],
    "Tuesday": [f"{h}:30" for h in range(19, 21)],
    "Wednesday": [],
    "Thursday": ["18:15", "19:15", "20:15", "21:15"],
    "Friday": [],
    "Saturday": [f"{h}:30" for h in range(16, 21)]
}

# ============================================================================
# ХРАНИЛИЩЕ ДАННЫХ
# ============================================================================

DATA_DIR = Path("bot_data")
DATA_DIR.mkdir(exist_ok=True)

STUDENTS_FILE = DATA_DIR / "students.json"
SCHEDULE_FILE = DATA_DIR / "schedule.json"
PENDING_FILE = DATA_DIR / "pending_requests.json"
CONFIRMED_FILE = DATA_DIR / "confirmed_lessons.json"
PENDING_RESCHEDULES_FILE = DATA_DIR / "pending_reschedules.json"
PENDING_CANCELS_FILE = DATA_DIR / "pending_cancels.json"

def load_json(filepath):
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def cleanup_stale_requests():
    """Удаляет pending-запросы старше 24 часов"""
    now = datetime.now()
    
    for filepath in [PENDING_FILE, PENDING_RESCHEDULES_FILE, PENDING_CANCELS_FILE]:
        data = load_json(filepath)
        stale_ids = []
        
        for req_id, req in data.items():
            try:
                req_time = datetime.fromisoformat(req.get("timestamp", ""))
                if (now - req_time).total_seconds() > 86400:  # 24 часа
                    stale_ids.append(req_id)
            except:
                pass
        
        for req_id in stale_ids:
            del data[req_id]
            print(f"🗑️  Удален старый запрос: {req_id}")
        
        if stale_ids:
            save_json(filepath, data)

# ============================================================================
# СОСТОЯНИЯ (FSM)
# ============================================================================

class FirstLessonStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_class = State()
    waiting_for_subject = State()
    waiting_for_time = State()

class RepeatLessonStates(StatesGroup):
    waiting_for_subject = State()
    waiting_for_time = State()

class TutorScheduleStates(StatesGroup):
    waiting_for_schedule_json = State()

class RescheduleStates(StatesGroup):
    choosing_lesson = State()
    waiting_for_new_time = State()

class CancelLessonStates(StatesGroup):
    choosing_lesson = State()

# ============================================================================
# КЛАВИАТУРЫ
# ============================================================================

def main_menu_keyboard(user_id: int):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎓 Первое занятие", callback_data="first_lesson")],
        [InlineKeyboardButton(text="📅 Повторное занятие", callback_data="repeat_lesson")],
        [InlineKeyboardButton(text="📍 Перенести занятие", callback_data="reschedule_lesson")],
        [InlineKeyboardButton(text="❌ Отменить занятие", callback_data="cancel_lesson")]
    ])
    if user_id == TUTOR_ID:
        kb.inline_keyboard.append(
            [InlineKeyboardButton(text="🛠 Изменить расписание", callback_data="edit_schedule")]
        )
    return kb

def quick_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📍 Перенести занятие", callback_data="reschedule_lesson")],
        [InlineKeyboardButton(text="❌ Отменить занятие", callback_data="cancel_lesson")],
        [InlineKeyboardButton(text="📅 Повторное занятие", callback_data="repeat_lesson")],
        [InlineKeyboardButton(text="🏠 Вернуться в начало", callback_data="back_to_menu")]
    ])

def persistent_menu_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="☰ Меню")]
    ], resize_keyboard=True, one_time_keyboard=False)

def subjects_keyboard_single():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s, callback_data=f"subject_single_{s}")] for s in SUBJECTS
    ])
    return kb

def tutor_confirm_keyboard(request_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{request_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{request_id}")]
    ])

def tutor_reschedule_confirm_keyboard(reschedule_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить перенос", callback_data=f"confirm_reschedule_{reschedule_id}")],
        [InlineKeyboardButton(text="❌ Отклонить перенос", callback_data=f"reject_reschedule_{reschedule_id}")]
    ])

def tutor_cancel_confirm_keyboard(cancel_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить отмену", callback_data=f"confirm_cancel_{cancel_id}")],
        [InlineKeyboardButton(text="❌ Отклонить отмену", callback_data=f"reject_cancel_{cancel_id}")]
    ])

def lessons_list_keyboard(lessons: Dict, action_type: str = "reschedule"):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for lesson_id, lesson in lessons.items():
        btn_text = f"{lesson['student_name']} - {lesson['date_str']} {lesson['time']}"
        callback = f"{action_type}_{lesson_id}"
        kb.inline_keyboard.append([
            InlineKeyboardButton(text=btn_text, callback_data=callback)
        ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")
    ])
    return kb

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def get_week_dates(start_date: datetime = None) -> Dict:
    if start_date is None:
        start_date = datetime.now()
    days_ahead = 0 - start_date.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    week_start = start_date + timedelta(days=days_ahead)
    
    days_map = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday"}
    days_ru = {"Monday": "Понедельник", "Tuesday": "Вторник", "Wednesday": "Среда", 
               "Thursday": "Четверг", "Friday": "Пятница", "Saturday": "Суббота"}
    
    week = {}
    for offset in range(6):
        date = week_start + timedelta(days=offset)
        day_name = days_map[date.weekday()]
        date_str = f"{date.strftime('%d %B')} ({days_ru[day_name]})"
        week[day_name] = (date, date_str)
    return week

def get_available_times(day_name: str, schedule: Dict) -> List[str]:
    return schedule.get(day_name, [])

def create_request_id():
    import uuid
    return str(uuid.uuid4())[:8]

def parse_time(time_str: str) -> tuple:
    parts = time_str.split(":")
    return int(parts[0]), int(parts[1])

def get_lesson_datetime(day_name: str, time_str: str) -> Optional[datetime]:
    week = get_week_dates()
    if day_name not in week:
        return None
    date_obj, _ = week[day_name]
    hour, minute = parse_time(time_str)
    return date_obj.replace(hour=hour, minute=minute, second=0, microsecond=0)

def get_student_lessons(student_id: int) -> Dict:
    confirmed = load_json(CONFIRMED_FILE)
    return {lid: l for lid, l in confirmed.items() if l["student_id"] == student_id}

def get_student_info(student_id: int) -> Optional[Dict]:
    students = load_json(STUDENTS_FILE)
    return students.get(str(student_id))

def save_student_info(student_id: int, name: str, grade: str):
    students = load_json(STUDENTS_FILE)
    students[str(student_id)] = {"name": name, "grade": grade}
    save_json(STUDENTS_FILE, students)

# ============================================================================
# ОБРАБОТЧИКИ
# ============================================================================

async def start_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Добро пожаловать! Выбери, что тебе нужно:",
        reply_markup=main_menu_keyboard(message.from_user.id)
    )
    await message.answer(
        "💡 Подсказка: нажми '☰ Меню' в любой момент для быстрого доступа",
        reply_markup=persistent_menu_keyboard()
    )

async def menu_button_handler(message: types.Message, state: FSMContext):
    if message.text == "☰ Меню":
        await state.clear()
        await message.answer(
            "📋 Быстрое меню:",
            reply_markup=quick_menu_keyboard()
        )
        return True
    return False

async def first_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("Введи своё имя:")
    await state.set_state(FirstLessonStates.waiting_for_name)

async def first_lesson_name_handler(message: types.Message, state: FSMContext):
    if await menu_button_handler(message, state):
        return
    await state.update_data(name=message.text)
    await message.answer("Введи номер класса (например, 9 или 10):")
    await state.set_state(FirstLessonStates.waiting_for_class)

async def first_lesson_class_handler(message: types.Message, state: FSMContext):
    if await menu_button_handler(message, state):
        return
    await state.update_data(grade=message.text)
    data = await state.get_data()
    save_student_info(message.from_user.id, data["name"], message.text)
    await message.answer(
        "Выбери предмет для занятия:",
        reply_markup=subjects_keyboard_single()
    )
    await state.set_state(FirstLessonStates.waiting_for_subject)

async def repeat_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    student_info = get_student_info(callback.from_user.id)
    if not student_info:
        await callback.message.edit_text(
            "❌ Ты ещё не записывался на первое занятие.\n\nПожалуйста, сначала запишись на первое занятие.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]])
        )
        return
    await state.update_data(name=student_info["name"], grade=student_info["grade"])
    await callback.message.edit_text(
        "Выбери предмет для занятия:",
        reply_markup=subjects_keyboard_single()
    )
    await state.set_state(RepeatLessonStates.waiting_for_subject)

async def reschedule_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    lessons = get_student_lessons(callback.from_user.id)
    if not lessons:
        await callback.answer()
        await callback.message.edit_text(
            "❌ У тебя нет подтверждённых занятий.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]])
        )
        return
    await callback.answer()
    await callback.message.edit_text(
        "Выбери занятие для переноса:",
        reply_markup=lessons_list_keyboard(lessons, "reschedule_pick")
    )
    await state.set_state(RescheduleStates.choosing_lesson)

async def reschedule_pick_handler(callback: types.CallbackQuery, state: FSMContext):
    lesson_id = callback.data.replace("reschedule_pick_", "")
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        return
    
    lesson = confirmed[lesson_id]
    await state.update_data(reschedule_lesson_id=lesson_id, selected_subject=lesson["subjects"][0])
    
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    if lesson["time"] in schedule.get(lesson["day"], []):
        pass
    else:
        schedule.setdefault(lesson["day"], []).append(lesson["time"])
    
    week = get_week_dates()
    message_text = "📅 Выбери новое время для занятия:\n\n"
    for day_name, (date, date_str) in week.items():
        times = get_available_times(day_name, schedule)
        if times:
            message_text += f"*{date_str}*\n"
            for time in times:
                message_text += f" • {time}\n"
            message_text += "\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for day_name, (date, date_str) in week.items():
        times = get_available_times(day_name, schedule)
        for time in times:
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"{date_str.split('(')[0].strip()} {time}",
                    callback_data=f"reschedule_time_{day_name}_{time}"
                )
            ])
    
    await callback.answer()
    await callback.message.edit_text(message_text, reply_markup=kb, parse_mode="Markdown")
    await state.set_state(RescheduleStates.waiting_for_new_time)

async def reschedule_time_handler(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace("reschedule_time_", "").split("_", 1)
    day_name = parts[0]
    time_slot = parts[1]
    
    data = await state.get_data()
    lesson_id = data.get("reschedule_lesson_id")
    
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        await state.clear()
        return
    
    lesson = confirmed[lesson_id]
    reschedule_id = create_request_id()
    
    week = get_week_dates()
    _, new_date_str = week[day_name]
    
    pending_reschedules = load_json(PENDING_RESCHEDULES_FILE)
    pending_reschedules[reschedule_id] = {
        "lesson_id": lesson_id,
        "student_id": callback.from_user.id,
        "student_name": lesson["student_name"],
        "old_day": lesson["day"],
        "old_time": lesson["time"],
        "old_date_str": lesson["date_str"],
        "new_day": day_name,
        "new_time": time_slot,
        "new_date_str": new_date_str,
        "subjects": lesson["subjects"],
        "status": "pending",
        "timestamp": datetime.now().isoformat()
    }
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    student_message = (
        f"✅ Запрос на перенос отправлен!\n\n"
        f"📚 {lesson['subjects'][0]}\n"
        f"📅 Было: {lesson['date_str']}\n"
        f"📅 Хотите перенести на: {new_date_str}\n"
        f"🕐 {time_slot}\n\n"
        f"⏳ Ожидается подтверждение от репетитора..."
    )
    
    tutor_reschedule = (
        f"📍 ЗАПРОС НА ПЕРЕНОС ЗАНЯТИЯ\n\n"
        f"👤 Ученик: {lesson['student_name']}\n"
        f"📚 {lesson['subjects'][0]}\n"
        f"📅 Было: {lesson['date_str']} в {lesson['time']}\n"
        f"📅 Просит перенести на: {new_date_str} в {time_slot}"
    )
    
    try:
        await callback.bot.send_message(
            TUTOR_ID,
            tutor_reschedule,
            reply_markup=tutor_reschedule_confirm_keyboard(reschedule_id)
        )
    except Exception as e:
        print(f"ERROR: {e}")
    
    await callback.answer()
    await callback.message.edit_text(student_message, reply_markup=None)
    await state.clear()

async def tutor_confirm_reschedule_handler(callback: types.CallbackQuery):
    reschedule_id = callback.data.replace("confirm_reschedule_", "")
    pending_reschedules = load_json(PENDING_RESCHEDULES_FILE)
    if reschedule_id not in pending_reschedules:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    reschedule = pending_reschedules[reschedule_id]
    lesson_id = reschedule["lesson_id"]
    
    confirmed = load_json(CONFIRMED_FILE)
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    
    if reschedule["old_time"] not in schedule.get(reschedule["old_day"], []):
        schedule.setdefault(reschedule["old_day"], []).append(reschedule["old_time"])
        schedule[reschedule["old_day"]].sort()
    
    if reschedule["new_time"] in schedule.get(reschedule["new_day"], []):
        schedule[reschedule["new_day"]].remove(reschedule["new_time"])
    
    save_json(SCHEDULE_FILE, schedule)
    
    if lesson_id in confirmed:
        confirmed[lesson_id]["day"] = reschedule["new_day"]
        confirmed[lesson_id]["time"] = reschedule["new_time"]
        confirmed[lesson_id]["date_str"] = reschedule["new_date_str"]
        confirmed[lesson_id]["reminder_sent"] = False
        save_json(CONFIRMED_FILE, confirmed)
    
    subject_str = ", ".join(reschedule["subjects"])
    
    student_message = (
        f"✅ ЗАНЯТИЕ ПЕРЕНЕСЕНО!\n\n"
        f"📚 {subject_str}\n"
        f"📅 Было: {reschedule['old_date_str']}\n"
        f"📅 Теперь: {reschedule['new_date_str']}\n"
        f"🕐 {reschedule['new_time']}"
    )
    
    tutor_confirmation = (
        f"✅ ПЕРЕНОС ЗАНЯТИЯ ПОДТВЕРЖДЕН\n\n"
        f"👤 Ученик: {reschedule['student_name']}\n"
        f"📚 {subject_str}\n"
        f"📅 Было: {reschedule['old_date_str']} в {reschedule['old_time']}\n"
        f"📅 Перенесено на: {reschedule['new_date_str']} в {reschedule['new_time']}"
    )
    
    try:
        await callback.bot.send_message(reschedule["student_id"], student_message)
    except:
        pass
    
    reschedule["status"] = "confirmed"
    pending_reschedules[reschedule_id] = reschedule
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    await callback.answer("✅ Перенос подтвержден!")
    await callback.message.edit_text(
        callback.message.text + "\n\n✅ *Перенос подтвержден*",
        parse_mode="Markdown"
    )

async def tutor_reject_reschedule_handler(callback: types.CallbackQuery):
    reschedule_id = callback.data.replace("reject_reschedule_", "")
    pending_reschedules = load_json(PENDING_RESCHEDULES_FILE)
    if reschedule_id not in pending_reschedules:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    reschedule = pending_reschedules[reschedule_id]
    
    student_message = (
        f"❌ К сожалению, перенос занятия не одобрен.\n\n"
        f"Попробуй выбрать другое время."
    )
    
    try:
        await callback.bot.send_message(reschedule["student_id"], student_message)
    except:
        pass
    
    reschedule["status"] = "rejected"
    pending_reschedules[reschedule_id] = reschedule
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    await callback.answer("❌ Перенос отклонен")
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Перенос отклонен*",
        parse_mode="Markdown"
    )

async def cancel_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    lessons = get_student_lessons(callback.from_user.id)
    if not lessons:
        await callback.answer()
        await callback.message.edit_text(
            "❌ У тебя нет подтверждённых занятий.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]])
        )
        return
    await callback.answer()
    await callback.message.edit_text(
        "Выбери занятие для отмены:",
        reply_markup=lessons_list_keyboard(lessons, "cancel_pick")
    )
    await state.set_state(CancelLessonStates.choosing_lesson)

async def cancel_pick_handler(callback: types.CallbackQuery, state: FSMContext):
    lesson_id = callback.data.replace("cancel_pick_", "")
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        return
    
    lesson = confirmed[lesson_id]
    cancel_id = create_request_id()
    
    pending_cancels = load_json(PENDING_CANCELS_FILE)
    pending_cancels[cancel_id] = {
        "lesson_id": lesson_id,
        "student_id": callback.from_user.id,
        "student_name": lesson["student_name"],
        "subjects": lesson["subjects"],
        "date_str": lesson["date_str"],
        "time": lesson["time"],
        "day": lesson["day"],
        "status": "pending",
        "timestamp": datetime.now().isoformat()
    }
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    subject_str = ", ".join(lesson["subjects"])
    
    student_message = (
        f"✅ Запрос на отмену отправлен!\n\n"
        f"📚 {subject_str}\n"
        f"📅 {lesson['date_str']}\n"
        f"🕐 {lesson['time']}\n\n"
        f"⏳ Ожидается подтверждение от репетитора..."
    )
    
    tutor_cancel_request = (
        f"❌ ЗАПРОС НА ОТМЕНУ ЗАНЯТИЯ\n\n"
        f"👤 Ученик: {lesson['student_name']}\n"
        f"📚 {subject_str}\n"
        f"📅 {lesson['date_str']}\n"
        f"🕐 {lesson['time']}"
    )
    
    try:
        await callback.bot.send_message(
            TUTOR_ID,
            tutor_cancel_request,
            reply_markup=tutor_cancel_confirm_keyboard(cancel_id)
        )
    except Exception as e:
        print(f"ERROR: {e}")
    
    await callback.answer()
    await callback.message.edit_text(student_message, reply_markup=None)
    await state.clear()

async def tutor_confirm_cancel_handler(callback: types.CallbackQuery):
    cancel_id = callback.data.replace("confirm_cancel_", "")
    pending_cancels = load_json(PENDING_CANCELS_FILE)
    if cancel_id not in pending_cancels:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    cancel = pending_cancels[cancel_id]
    lesson_id = cancel["lesson_id"]
    
    confirmed = load_json(CONFIRMED_FILE)
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    
    if cancel["time"] not in schedule.get(cancel["day"], []):
        schedule.setdefault(cancel["day"], []).append(cancel["time"])
        schedule[cancel["day"]].sort()
    save_json(SCHEDULE_FILE, schedule)
    
    if lesson_id in confirmed:
        del confirmed[lesson_id]
        save_json(CONFIRMED_FILE, confirmed)
    
    subject_str = ", ".join(cancel["subjects"])
    
    student_message = (
        f"✅ ЗАНЯТИЕ ОТМЕНЕНО\n\n"
        f"📚 {subject_str}\n"
        f"📅 {cancel['date_str']}\n"
        f"🕐 {cancel['time']}\n\n"
        f"Время возвращено в доступное расписание."
    )
    
    tutor_confirmation = (
        f"✅ ОТМЕНА ЗАНЯТИЯ ПОДТВЕРЖДЕНА\n\n"
        f"👤 Ученик: {cancel['student_name']}\n"
        f"📚 {subject_str}\n"
        f"📅 {cancel['date_str']}\n"
        f"🕐 {cancel['time']}\n\n"
        f"Время освобождено в расписании."
    )
    
    try:
        await callback.bot.send_message(cancel["student_id"], student_message)
    except:
        pass
    
    cancel["status"] = "confirmed"
    pending_cancels[cancel_id] = cancel
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    await callback.answer("✅ Отмена подтверждена!")
    await callback.message.edit_text(
        callback.message.text + "\n\n✅ *Отмена подтверждена*",
        parse_mode="Markdown"
    )

async def tutor_reject_cancel_handler(callback: types.CallbackQuery):
    cancel_id = callback.data.replace("reject_cancel_", "")
    pending_cancels = load_json(PENDING_CANCELS_FILE)
    if cancel_id not in pending_cancels:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    cancel = pending_cancels[cancel_id]
    
    student_message = (
        f"❌ К сожалению, отмена занятия не одобрена.\n\n"
        f"Занятие остаётся в расписании."
    )
    
    try:
        await callback.bot.send_message(cancel["student_id"], student_message)
    except:
        pass
    
    cancel["status"] = "rejected"
    pending_cancels[cancel_id] = cancel
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    await callback.answer("❌ Отмена отклонена")
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Отмена отклонена*",
        parse_mode="Markdown"
    )

async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text(
        "👋 Добро пожаловать! Выбери, что тебе нужно:",
        reply_markup=main_menu_keyboard(callback.from_user.id)
    )

async def subject_single_handler(callback: types.CallbackQuery, state: FSMContext):
    subject = callback.data.replace("subject_single_", "")
    await state.update_data(selected_subject=subject)
    
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    week = get_week_dates()
    
    message_text = "📅 Выбери день и время для занятия:\n\n"
    for day_name, (date, date_str) in week.items():
        times = get_available_times(day_name, schedule)
        if times:
            message_text += f"*{date_str}*\n"
            for time in times:
                message_text += f" • {time}\n"
            message_text += "\n"
        else:
            message_text += f"❌ {date_str} — нет свободного времени\n\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for day_name, (date, date_str) in week.items():
        times = get_available_times(day_name, schedule)
        for time in times:
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"{date_str.split('(')[0].strip()} {time}",
                    callback_data=f"time_{day_name}_{time}"
                )
            ])
    
    await callback.answer()
    await callback.message.edit_text(message_text, reply_markup=kb, parse_mode="Markdown")
    current_state = await state.get_state()
    if current_state == FirstLessonStates.waiting_for_subject:
        await state.set_state(FirstLessonStates.waiting_for_time)
    else:
        await state.set_state(RepeatLessonStates.waiting_for_time)

async def time_select_handler(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace("time_", "").split("_", 1)
    day_name = parts[0]
    time_slot = parts[1]
    
    data = await state.get_data()
    selected_subject = data.get("selected_subject", "Неизвестно")
    student_name = data.get("name", "Неизвестно")
    student_grade = data.get("grade", "Неизвестно")
    
    week = get_week_dates()
    date_obj, date_str = week[day_name]
    
    student_message = (
        f"✅ Занятие зарегистрировано!\n\n"
        f"📚 Предмет: {selected_subject}\n"
        f"📅 {date_str}\n"
        f"🕐 {time_slot}\n\n"
        f"⏳ Ожидается подтверждение от репетитора..."
    )
    
    tutor_message = (
        f"📬 НОВЫЙ ЗАПРОС НА ЗАНЯТИЕ\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📖 Класс: {student_grade}\n"
        f"📚 Предмет: {selected_subject}\n"
        f"📅 {date_str}\n"
        f"🕐 {time_slot}\n"
    )
    
    request_id = create_request_id()
    pending = load_json(PENDING_FILE)
    pending[request_id] = {
        "student_id": callback.from_user.id,
        "student_name": student_name,
        "grade": student_grade,
        "subjects": [selected_subject],
        "day": day_name,
        "time": time_slot,
        "date_str": date_str,
        "status": "pending",
        "timestamp": datetime.now().isoformat()
    }
    save_json(PENDING_FILE, pending)
    
    try:
        await callback.bot.send_message(
            TUTOR_ID,
            tutor_message,
            reply_markup=tutor_confirm_keyboard(request_id)
        )
    except Exception as e:
        print(f"ERROR: {e}")
    
    await callback.answer()
    await callback.message.edit_text(student_message, reply_markup=None)
    await state.clear()

async def tutor_confirm_handler(callback: types.CallbackQuery):
    request_id = callback.data.replace("confirm_", "")
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("ERROR: Request not found", show_alert=True)
        return
    
    request = pending[request_id]
    student_id = request["student_id"]
    subjects_str = ", ".join(request["subjects"])
    
    student_message = (
        f"✅ ПОДТВЕРЖДЕНО!\n\n"
        f"📚 Занятие {subjects_str}\n"
        f"📅 {request['date_str']}\n"
        f"🕐 {request['time']}\n\n"
        f"Подготовь домашнее задание и вопросы к занятию!"
    )
    
    tutor_confirmation = (
        f"✅ ЗАНЯТИЕ ПОДТВЕРЖДЕНО\n\n"
        f"👤 Ученик: {request['student_name']}\n"
        f"📚 {subjects_str}\n"
        f"📅 {request['date_str']}\n"
        f"🕐 {request['time']}\n\n"
        f"Ученик получил уведомление о подтверждении."
    )
    
    try:
        await callback.bot.send_message(student_id, student_message)
    except:
        pass
    
    try:
        await callback.bot.send_message(TUTOR_ID, tutor_confirmation)
    except:
        pass
    
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    day_name = request["day"]
    time_slot = request["time"]
    if time_slot in schedule.get(day_name, []):
        schedule[day_name].remove(time_slot)
    save_json(SCHEDULE_FILE, schedule)
    
    confirmed = load_json(CONFIRMED_FILE)
    confirmed[request_id] = {
        "student_id": student_id,
        "student_name": request["student_name"],
        "subjects": request["subjects"],
        "day": day_name,
        "time": time_slot,
        "date_str": request["date_str"],
        "reminder_sent": False,
        "timestamp": datetime.now().isoformat()
    }
    save_json(CONFIRMED_FILE, confirmed)
    
    request["status"] = "confirmed"
    pending[request_id] = request
    save_json(PENDING_FILE, pending)
    
    await callback.answer("✅ Занятие подтверждено!")
    await callback.message.edit_text(
        callback.message.text + "\n\n✅ *Занятие подтверждено и добавлено в расписание*",
        parse_mode="Markdown"
    )

async def tutor_reject_handler(callback: types.CallbackQuery):
    request_id = callback.data.replace("reject_", "")
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("ERROR: Request not found", show_alert=True)
        return
    
    request = pending[request_id]
    student_id = request["student_id"]
    
    student_message = (
        f"❌ К сожалению, в выбранное время занятие провести не получится.\n\n"
        f"Попробуй выбрать другое время или день."
    )
    
    tutor_rejection = (
        f"❌ ЗАНЯТИЕ ОТКЛОНЕНО\n\n"
        f"👤 Ученик: {request['student_name']}\n"
        f"📚 {', '.join(request['subjects'])}\n"
        f"📅 {request['date_str']}\n"
        f"🕐 {request['time']}\n\n"
        f"Ученик получил уведомление об отклонении и может выбрать другое время."
    )
    
    try:
        await callback.bot.send_message(student_id, student_message)
    except:
        pass
    
    try:
        await callback.bot.send_message(TUTOR_ID, tutor_rejection)
    except:
        pass
    
    request["status"] = "rejected"
    pending[request_id] = request
    save_json(PENDING_FILE, pending)
    
    await callback.answer("❌ Запрос отклонен")
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Занятие отклонено*",
        parse_mode="Markdown"
    )

async def edit_schedule_button_handler(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TUTOR_ID:
        await callback.answer("🔒 Эта функция доступна только репетитору.", show_alert=True)
        return
    
    await callback.answer()
    help_text = (
        "📝 *Изменение расписания*\n\n"
        "Отправь новое расписание в формате JSON.\n\n"
        "Пример:\n"
        "```json\n"
        "{\n"
        ' "Monday": ["18:30", "19:30"],\n'
        ' "Tuesday": ["19:30"],\n'
        ' "Wednesday": [],\n'
        ' "Thursday": ["18:15"],\n'
        ' "Friday": [],\n'
        ' "Saturday": ["16:30"]\n'
        "}\n"
        "```"
    )
    await callback.message.edit_text(help_text, parse_mode="Markdown")
    await state.set_state(TutorScheduleStates.waiting_for_schedule_json)

async def schedule_json_handler(message: types.Message, state: FSMContext):
    try:
        schedule_data = json.loads(message.text)
        required_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        for day in required_days:
            if day not in schedule_data:
                raise ValueError(f"День '{day}' отсутствует")
        save_json(SCHEDULE_FILE, schedule_data)
        await message.answer("✅ Расписание обновлено!")
        await state.clear()
    except json.JSONDecodeError:
        await message.answer("❌ Ошибка в формате JSON. Попробуй ещё раз.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

async def send_daily_schedule(bot: Bot):
    """Отправка расписания на день в 8:00 утра"""
    while True:
        try:
            now = datetime.now()
            if now.hour == 8 and now.minute < 5:
                confirmed = load_json(CONFIRMED_FILE)
                today_date = now.strftime('%d')
                today_lessons = []
                
                for lesson_id, lesson in confirmed.items():
                    try:
                        lesson_date = datetime.strptime(lesson['date_str'].split(' (')[0], '%d %B')
                        if lesson_date.day == int(today_date):
                            today_lessons.append((lesson['time'], lesson))
                    except:
                        pass
                
                if today_lessons:
                    today_lessons.sort(key=lambda x: x[0])
                    message = "📅 РАСПИСАНИЕ НА СЕГОДНЯ\n\n"
                    for time_str, lesson in today_lessons:
                        message += f"🕐 {time_str} - {lesson['student_name']} ({lesson['subjects'][0]})\n"
                    
                    try:
                        await bot.send_message(TUTOR_ID, message)
                    except:
                        pass
            await asyncio.sleep(60)
        except:
            await asyncio.sleep(60)

async def send_reminders(bot: Bot):
    """Отправка напоминания за 1 час до занятия"""
    while True:
        try:
            confirmed = load_json(CONFIRMED_FILE)
            now = datetime.now()
            for lesson_id, lesson in confirmed.items():
                if lesson.get("reminder_sent"):
                    continue
                lesson_datetime = get_lesson_datetime(lesson["day"], lesson["time"])
                if not lesson_datetime:
                    continue
                time_until_lesson = lesson_datetime - now
                if timedelta(minutes=59) <= time_until_lesson <= timedelta(minutes=61):
                    subjects_str = ", ".join(lesson["subjects"])
                    student_reminder = (
                        f"⏰ НАПОМИНАНИЕ О ЗАНЯТИИ!\n\n"
                        f"📚 {subjects_str}\n"
                        f"📅 {lesson['date_str']}\n"
                        f"🕐 {lesson['time']}\n\n"
                        f"Начало через 1 час! Подготовь все необходимое!"
                    )
                    tutor_reminder = (
                        f"⏰ НАПОМИНАНИЕ О ЗАНЯТИИ!\n\n"
                        f"👤 Ученик: {lesson['student_name']}\n"
                        f"📚 {subjects_str}\n"
                        f"📅 {lesson['date_str']}\n"
                        f"🕐 {lesson['time']}\n\n"
                        f"Начало через 1 час!"
                    )
                    try:
                        await bot.send_message(lesson["student_id"], student_reminder)
                    except:
                        pass
                    try:
                        await bot.send_message(TUTOR_ID, tutor_reminder)
                    except:
                        pass
                    lesson["reminder_sent"] = True
                    confirmed[lesson_id] = lesson
                    save_json(CONFIRMED_FILE, confirmed)
            await asyncio.sleep(30)
        except:
            await asyncio.sleep(30)

async def cleanup_task(bot: Bot):
    """Очистка старых данных каждый час"""
    while True:
        try:
            await asyncio.sleep(3600)
            cleanup_stale_requests()
            print(f"✅ Очистка старых данных завершена [{datetime.now().strftime('%H:%M:%S')}]")
        except Exception as e:
            print(f"Cleanup task error: {e}")
            await asyncio.sleep(300)

async def keep_alive_task():
    """Периодический keep-alive запрос к своему серверу"""
    if not RENDER_URL:
        print("⚠️  RENDER_URL не установлен. Keep-alive отключен.")
        return
    
    await asyncio.sleep(30)  # Начинаем после инициализации
    
    while True:
        try:
            await asyncio.sleep(840)  # 14 минут (меньше лимита 15 мин)
            
            async with ClientSession() as session:
                try:
                    async with session.get(f"{RENDER_URL}/health", timeout=5) as resp:
                        if resp.status == 200:
                            print(f"✅ Keep-alive ping успешен [{datetime.now().strftime('%H:%M:%S')}]")
                        else:
                            print(f"⚠️  Keep-alive ответ: {resp.status}")
                except Exception as e:
                    print(f"⚠️  Keep-alive ошибка: {e}")
        except Exception as e:
            print(f"❌ Keep-alive task error: {e}")
            await asyncio.sleep(60)

# ============================================================================
# HTTP СЕРВЕР
# ============================================================================

async def health_handler(request):
    return web.json_response({"status": "ok", "service": "tutor_bot", "timestamp": datetime.now().isoformat()})

async def root_handler(request):
    return web.Response(text="Bot is running!", status=200)

async def run_http_server():
    try:
        print("Creating HTTP application...")
        app = web.Application()
        app.router.add_get('/', root_handler)
        app.router.add_get('/health', health_handler)
        print("OK: HTTP application created")
        
        print(f"Starting HTTP server on 0.0.0.0:{PORT}...")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        
        print(f"OK: HTTP server started on 0.0.0.0:{PORT}")
        print("=" * 70)
        print("BOT IS READY")
        print("=" * 70)
        sys.stdout.flush()
        
        await asyncio.sleep(float('inf'))
    except Exception as e:
        print(f"ERROR: HTTP server error: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# БОТ
# ============================================================================

async def start_bot():
    """Запуск бота с защитой от конфликтов"""
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        try:
            print("Initializing Telegram bot...")
            print("Creating bot...")
            bot = Bot(token=TOKEN)
            storage = MemoryStorage()
            dp = Dispatcher(storage=storage)
            print("OK: Dispatcher created")
            
            print("Registering handlers...")
            
            dp.message.register(start_handler, Command("start"))
            dp.message.register(menu_button_handler, F.text == "☰ Меню")
            dp.message.register(first_lesson_name_handler, FirstLessonStates.waiting_for_name)
            dp.message.register(first_lesson_class_handler, FirstLessonStates.waiting_for_class)
            dp.message.register(schedule_json_handler, TutorScheduleStates.waiting_for_schedule_json)
            
            dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")
            dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")
            dp.callback_query.register(reschedule_lesson_handler, F.data == "reschedule_lesson")
            dp.callback_query.register(cancel_lesson_handler, F.data == "cancel_lesson")
            dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")
            dp.callback_query.register(subject_single_handler, F.data.startswith("subject_single_"))
            dp.callback_query.register(time_select_handler, F.data.startswith("time_"))
            dp.callback_query.register(reschedule_pick_handler, F.data.startswith("reschedule_pick_"))
            dp.callback_query.register(reschedule_time_handler, F.data.startswith("reschedule_time_"))
            dp.callback_query.register(tutor_confirm_reschedule_handler, F.data.startswith("confirm_reschedule_"))
            dp.callback_query.register(tutor_reject_reschedule_handler, F.data.startswith("reject_reschedule_"))
            dp.callback_query.register(cancel_pick_handler, F.data.startswith("cancel_pick_"))
            dp.callback_query.register(tutor_confirm_cancel_handler, F.data.startswith("confirm_cancel_"))
            dp.callback_query.register(tutor_reject_cancel_handler, F.data.startswith("reject_cancel_"))
            dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
            dp.callback_query.register(tutor_confirm_handler, F.data.startswith("confirm_"))
            dp.callback_query.register(tutor_reject_handler, F.data.startswith("reject_"))
            
            print("OK: Handlers registered")
            print("Waiting for messages from Telegram...\n")
            sys.stdout.flush()
            
            retry_count = 0
            
            asyncio.create_task(send_reminders(bot))
            asyncio.create_task(send_daily_schedule(bot))
            asyncio.create_task(cleanup_task(bot))
            asyncio.create_task(keep_alive_task())
            
            await dp.start_polling(bot, skip_updates=True, handle_signals=False)
            
        except Exception as e:
            error_msg = str(e).lower()
            
            if "conflict" in error_msg or "getupdates" in error_msg:
                retry_count += 1
                wait_time = min(10 * (2 ** retry_count), 600)
                print(f"\n⚠️  TelegramConflictError! Попытка перезапуска {retry_count}/{max_retries}")
                print(f"   Ожидаю {wait_time} секунд перед перезапуском...")
                sys.stdout.flush()
                await asyncio.sleep(wait_time)
                continue
            
            print(f"ERROR: Bot error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)
            continue
    
    if retry_count >= max_retries:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось запустить бота после {max_retries} попыток")
        print("❌ Возможно, другой экземпляр бота уже запущен")
        sys.exit(1)

# ============================================================================
# MAIN
# ============================================================================

async def main():
    print("=" * 70)
    print("INITIALIZING APPLICATION")
    print("=" * 70)
    print(f"Port: {PORT}")
    print(f"Token: {'OK' if TOKEN else 'NOT SET'}")
    print(f"Render URL: {RENDER_URL if RENDER_URL else 'NOT SET (keep-alive disabled)'}")
    print("=" * 70 + "\n")
    sys.stdout.flush()
    
    lock_file = Path(".bot_running.lock")
    
    if lock_file.exists():
        print("🔄 Обнаружен старый процесс бота. Очищаю...")
        try:
            lock_file.unlink()
        except Exception as e:
            print(f"Warning: Could not delete old lock file: {e}")
    
    lock_file.write_text(str(os.getpid()))
    print(f"✅ Lock file created: {lock_file}\n")
    
    print("🧹 Performing startup cleanup...")
    cleanup_stale_requests()
    print("✅ Startup cleanup completed\n")
    sys.stdout.flush()
    
    try:
        await asyncio.gather(
            run_http_server(),
            start_bot()
        )
    except KeyboardInterrupt:
        print("\n⏹️  Application interrupted by user")
    except Exception as e:
        print(f"ERROR: Main thread error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if lock_file.exists():
            try:
                lock_file.unlink()
                print("✅ Lock file removed")
            except:
                pass
        print("\n✅ Bot stopped correctly")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication stopped")
    except Exception as e:
        print(f"ERROR: Main thread error: {e}")
        import traceback
        traceback.print_exc()
