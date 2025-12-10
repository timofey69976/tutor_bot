# -*- coding: utf-8 -*-
"""
Telegram бот для управления расписанием занятий репетитора
Адаптирован для работы на Render с HTTP сервером
"""

import os
import asyncio
import sys
import threading
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from aiohttp import web
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

def load_json(filepath):
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
    """Выбор ОДНОГО предмета"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s, callback_data=f"subject_single_{s}") for s in SUBJECTS]
    ])
    return kb

def tutor_confirm_keyboard(request_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{request_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{request_id}")]
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
    await callback.answer()
    lessons = get_student_lessons(callback.from_user.id)
    if not lessons:
        await callback.message.edit_text(
            "❌ У тебя нет подтверждённых занятий.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]])
        )
        return
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
    schedule[lesson["day"]].append(lesson["time"])
    
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
    lesson = confirmed[lesson_id]
    
    old_day = lesson["day"]
    old_time = lesson["time"]
    
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    if time_slot in schedule.get(day_name, []):
        schedule[day_name].remove(time_slot)
    if old_time in schedule.get(old_day, []):
        schedule[old_day].append(old_time)
    save_json(SCHEDULE_FILE, schedule)
    
    week = get_week_dates()
    _, new_date_str = week[day_name]
    
    lesson["day"] = day_name
    lesson["time"] = time_slot
    lesson["date_str"] = new_date_str
    confirmed[lesson_id] = lesson
    save_json(CONFIRMED_FILE, confirmed)
    
    student_message = (
        f"✅ ЗАНЯТИЕ ПЕРЕНЕСЕНО!\n\n"
        f"📚 {lesson['subjects'][0]}\n"
        f"📅 {new_date_str}\n"
        f"🕐 {time_slot}"
    )
    
    try:
        await callback.bot.send_message(lesson["student_id"], student_message)
    except:
        pass
    
    await callback.message.edit_text(
        f"✅ Занятие успешно перенесено на {new_date_str} {time_slot}",
        reply_markup=None
    )
    await callback.answer()
    await state.clear()

async def cancel_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    lessons = get_student_lessons(callback.from_user.id)
    if not lessons:
        await callback.message.edit_text(
            "❌ У тебя нет подтверждённых занятий.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]])
        )
        return
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
    
    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    if lesson["time"] not in schedule.get(lesson["day"], []):
        schedule.setdefault(lesson["day"], []).append(lesson["time"])
        schedule[lesson["day"]].sort()
    save_json(SCHEDULE_FILE, schedule)
    
    del confirmed[lesson_id]
    save_json(CONFIRMED_FILE, confirmed)
    
    tutor_message = (
        f"❌ ЗАНЯТИЕ ОТМЕНЕНО\n\n"
        f"👤 Ученик: {lesson['student_name']}\n"
        f"📚 {lesson['subjects'][0]}\n"
        f"📅 {lesson['date_str']}\n"
        f"🕐 {lesson['time']}"
    )
    
    try:
        await callback.bot.send_message(TUTOR_ID, tutor_message)
    except:
        pass
    
    await callback.message.edit_text(
        f"✅ Занятие отменено",
        reply_markup=None
    )
    await callback.answer()
    await state.clear()

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
    
    await callback.message.edit_text(message_text, reply_markup=kb, parse_mode="Markdown")
    current_state = await state.get_state()
    if current_state == FirstLessonStates.waiting_for_subject:
        await state.set_state(FirstLessonStates.waiting_for_time)
    else:
        await state.set_state(RepeatLessonStates.waiting_for_time)
    await callback.answer()

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
    
    await state.update_data(
        selected_day=day_name,
        selected_time=time_slot,
        selected_date_str=date_str
    )
    
    student_message = (
        f"✅ Занятие зарегистрировано!\n\n"
        f"📚 Предмет: {selected_subject}\n"
        f"📅 {date_str}\n"
        f"🕐 {time_slot}\n\n"
        f"⏳ Ожидается подтверждение от репетитора..."
    )
    
    await callback.message.edit_text(student_message, reply_markup=None)
    
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
    
    try:
        await callback.bot.send_message(student_id, student_message)
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
    
    await callback.message.edit_text(
        callback.message.text + "\n\n✅ *Lesson confirmed and added to schedule*",
        parse_mode="Markdown"
    )
    await callback.answer("✅ Lesson confirmed!")

async def tutor_reject_handler(callback: types.CallbackQuery):
    request_id = callback.data.replace("reject_", "")
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("ERROR: Request not found", show_alert=True)
        return
    
    request = pending[request_id]
    student_id = request["student_id"]
    
    student_message = (
        f"❌ Unfortunately, we cannot conduct a lesson at the selected time.\n\n"
        f"Please try to select a different time or day."
    )
    
    try:
        await callback.bot.send_message(student_id, student_message)
    except:
        pass
    
    request["status"] = "rejected"
    pending[request_id] = request
    save_json(PENDING_FILE, pending)
    
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Lesson rejected*",
        parse_mode="Markdown"
    )
    await callback.answer("❌ Request rejected")

async def edit_schedule_button_handler(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TUTOR_ID:
        await callback.answer("LOCKED: Available for tutor only.", show_alert=True)
        return
    
    await callback.answer()
    help_text = (
        "*Schedule update*\n\n"
        "Send the new schedule in JSON format.\n\n"
        "Example:\n"
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
                raise ValueError(f"Day '{day}' is missing")
        save_json(SCHEDULE_FILE, schedule_data)
        await message.answer("✅ Schedule updated!")
        await state.clear()
    except json.JSONDecodeError:
        await message.answer("ERROR: Invalid JSON format. Try again.")
    except Exception as e:
        await message.answer(f"ERROR: {e}")

async def send_reminders(bot: Bot):
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
                        f"REMINDER ABOUT LESSON!\n\n"
                        f"{subjects_str}\n"
                        f"{lesson['date_str']}\n"
                        f"{lesson['time']}\n\n"
                        f"Starts in 1 hour! Prepare everything needed!"
                    )
                    tutor_reminder = (
                        f"REMINDER ABOUT LESSON!\n\n"
                        f"Student: {lesson['student_name']}\n"
                        f"{subjects_str}\n"
                        f"{lesson['date_str']}\n"
                        f"{lesson['time']}\n\n"
                        f"Starts in 1 hour!"
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

# ============================================================================
# HTTP SERVER
# ============================================================================

async def health_handler(request):
    return web.json_response({"status": "ok", "service": "tutor_bot"})

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
# BOT
# ============================================================================

async def start_bot():
    """Run bot without threading - polling only"""
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
        dp.callback_query.register(cancel_pick_handler, F.data.startswith("cancel_pick_"))
        dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
        dp.callback_query.register(tutor_confirm_handler, F.data.startswith("confirm_"))
        dp.callback_query.register(tutor_reject_handler, F.data.startswith("reject_"))
        
        print("OK: Handlers registered")
        print("Waiting for messages from Telegram...\n")
        sys.stdout.flush()
        
        asyncio.create_task(send_reminders(bot))
        await dp.start_polling(bot, skip_updates=True, handle_signals=False)
        
    except Exception as e:
        print(f"ERROR: Bot error: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# MAIN
# ============================================================================

async def main():
    print("=" * 70)
    print("INITIALIZING APPLICATION")
    print("=" * 70)
    print(f"Port: {PORT}")
    print(f"Token: {'OK' if TOKEN else 'NOT SET'}")
    print("=" * 70 + "\n")
    sys.stdout.flush()
    
    await asyncio.gather(
        run_http_server(),
        start_bot()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication stopped")
    except Exception as e:
        print(f"ERROR: Main thread error: {e}")
        import traceback
        traceback.print_exc()
