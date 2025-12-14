# -*- coding: utf-8 -*-

"""
Telegram бот для управления расписанием занятий репетитора
ПОЛНАЯ СИСТЕМА: все функции работают, все данные ученика сохраняются
ИСПРАВЛЕНО: кнопки подтверждения для переноса и отмены, данные в повторных занятиях
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
RENDER_URL = os.getenv('RENDER_URL', '')

if not TOKEN:
    TOKEN = '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI'

TUTOR_ID = 1339816111
SUBJECTS = ["Математика", "Физика", "Химия"]
DEFAULT_SCHEDULE = {
    "Monday": [f"{h}:00" for h in range(18, 21)],
    "Tuesday": [f"{h}:30" for h in range(19, 21)],
    "Wednesday": [],
    "Thursday": ["18:15", "19:15", "20:15"],
    "Friday": [],
    "Saturday": [f"{h}:30" for h in range(16, 21)]
}

# ============================================================================
# ИНТЕРАКТИВНОЕ РАСПИСАНИЕ - КОНФИГУРАЦИЯ
# ============================================================================

SLOT_DURATION = 60
MAX_WORK_HOUR = 21
MAX_WORK_MINUTE = 0

DAYS_RU = {
    "Monday": "Понедельник",
    "Tuesday": "Вторник",
    "Wednesday": "Среда",
    "Thursday": "Четверг",
    "Friday": "Пятница",
    "Saturday": "Суббота"
}

DAYS_EMOJI = {
    "Monday": "📅",
    "Tuesday": "📅",
    "Wednesday": "📅",
    "Thursday": "📅",
    "Friday": "📅",
    "Saturday": "📅"
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
                if (now - req_time).total_seconds() > 86400:
                    stale_ids.append(req_id)
            except:
                pass
        
        for req_id in stale_ids:
            del data[req_id]
            print(f"🗑️ Удален старый запрос: {req_id}")
        
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

class MyScheduleStates(StatesGroup):
    viewing_schedule = State()

class InteractiveScheduleStates(StatesGroup):
    choosing_day = State()
    waiting_for_start_time = State()
    editing_day = State()
    confirming_all = State()

# ============================================================================
# КЛАВИАТУРЫ
# ============================================================================

def main_menu_keyboard(user_id: int):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎓 Первое занятие", callback_data="first_lesson")],
        [InlineKeyboardButton(text="📅 Повторное занятие", callback_data="repeat_lesson")],
        [InlineKeyboardButton(text="📍 Перенести занятие", callback_data="reschedule_lesson")],
        [InlineKeyboardButton(text="❌ Отменить занятие", callback_data="cancel_lesson")],
        [InlineKeyboardButton(text="📚 Мое расписание", callback_data="my_schedule")]
    ])
    
    if user_id == TUTOR_ID:
        kb.inline_keyboard.append(
            [InlineKeyboardButton(text="🛠 Изменить расписание", callback_data="edit_schedule")]
        )
    
    return kb

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
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_reschedule_{reschedule_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_reschedule_{reschedule_id}")]
    ])

def tutor_cancel_confirm_keyboard(cancel_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_cancel_{cancel_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_cancel_{cancel_id}")]
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
    days_ru = {
        "Monday": "Понедельник",
        "Tuesday": "Вторник",
        "Wednesday": "Среда",
        "Thursday": "Четверг",
        "Friday": "Пятница",
        "Saturday": "Суббота"
    }
    
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

def get_tutor_lessons() -> Dict:
    """Получить все занятия репетитора на текущую неделю"""
    confirmed = load_json(CONFIRMED_FILE)
    week = get_week_dates()
    
    tutor_lessons = {}
    for lesson_id, lesson in confirmed.items():
        try:
            lesson_date = datetime.fromisoformat(lesson["lesson_datetime"])
            week_start = week["Monday"][0]
            week_end = week["Saturday"][0] + timedelta(days=1)
            
            if week_start <= lesson_date < week_end:
                tutor_lessons[lesson_id] = lesson
        except:
            pass
    
    return tutor_lessons

def save_student_info(student_id: int, name: str, grade: str):
    """Сохраняет информацию о ученике"""
    students = load_json(STUDENTS_FILE)
    students[str(student_id)] = {"name": name, "grade": grade}
    save_json(STUDENTS_FILE, students)

def get_student_info(student_id: int) -> Optional[Dict]:
    """Получает информацию о ученике"""
    students = load_json(STUDENTS_FILE)
    return students.get(str(student_id))

def format_student_schedule_message(lessons: Dict) -> str:
    """Форматирует расписание для ученика"""
    if not lessons:
        return "📭 У вас нет занятий на эту неделю."
    
    message = "📚 <b>Ваше расписание на эту неделю:</b>\n\n"
    
    sorted_lessons = sorted(lessons.values(), key=lambda x: x.get("lesson_datetime", ""))
    
    for lesson in sorted_lessons:
        try:
            lesson_date = datetime.fromisoformat(lesson["lesson_datetime"])
            date_str = lesson_date.strftime("%d.%m.%Y")
            time_str = lesson_date.strftime("%H:%M")
            subject = lesson.get("subject", "Неизвестный предмет")
            
            message += f"📅 <b>{date_str}</b> в <b>{time_str}</b>\n"
            message += f"   Предмет: {subject}\n"
            message += f"   Статус: ✅ Подтверждено\n\n"
        except:
            pass
    
    return message

def format_tutor_schedule_message(lessons: Dict) -> str:
    """Форматирует расписание для репетитора"""
    if not lessons:
        return "📭 У вас нет занятий на эту неделю."
    
    message = "📚 <b>Ваше расписание на эту неделю:</b>\n\n"
    
    sorted_lessons = sorted(lessons.values(), key=lambda x: x.get("lesson_datetime", ""))
    
    for lesson in sorted_lessons:
        try:
            lesson_date = datetime.fromisoformat(lesson["lesson_datetime"])
            date_str = lesson_date.strftime("%d.%m.%Y")
            time_str = lesson_date.strftime("%H:%M")
            student_name = lesson.get("student_name", "Неизвестный ученик")
            subject = lesson.get("subject", "Неизвестный предмет")
            
            message += f"📅 <b>{date_str}</b> в <b>{time_str}</b>\n"
            message += f"   Ученик: {student_name}\n"
            message += f"   Предмет: {subject}\n"
            message += f"   Статус: ✅ Подтверждено\n\n"
        except:
            pass
    
    return message

def parse_time_input(text: str) -> Optional[Tuple[int, int]]:
    """Парсит ввод времени: '19:30' или '19' -> (19, 30)"""
    text = text.strip()
    
    if text.lower() in ['нет', 'no', '-', 'skip']:
        return None
    
    try:
        if ':' in text:
            parts = text.split(':')
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
        else:
            h = int(text)
            m = 0
        
        if 0 <= h < 24 and 0 <= m < 60:
            return (h, m)
    except:
        pass
    
    return "invalid"


def generate_time_slots(start_hour: int, start_minute: int) -> List[str]:
    """Генерирует слоты на основе времени начала"""
    slots = []
    current_hour = start_hour
    current_minute = start_minute
    
    max_minutes = MAX_WORK_HOUR * 60 + MAX_WORK_MINUTE
    
    while True:
        current_minutes = current_hour * 60 + current_minute
        
        if current_minutes > max_minutes:
            break
        
        if current_hour >= 24:
            break
        
        time_str = f"{current_hour:02d}:{current_minute:02d}"
        slots.append(time_str)
        
        current_minute += SLOT_DURATION
        if current_minute >= 60:
            current_hour += current_minute // 60
            current_minute = current_minute % 60
    
    return slots


def format_schedule_for_preview(schedule_dict: Dict) -> str:
    """Форматирует расписание для предпросмотра"""
    message = "📋 <b>Ваше расписание:</b>\n\n"
    
    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
        day_ru = DAYS_RU.get(day_name, day_name)
        times = schedule_dict.get(day_name, [])
        
        if isinstance(times, str) and times == "нет":
            times_str = "❌ нет занятий"
        elif times:
            times_str = ", ".join(times)
        else:
            times_str = "⏳ не установлено"
        
        message += f"{DAYS_EMOJI[day_name]} <b>{day_ru}:</b> {times_str}\n"
    
    return message

# ============================================================================
# ОБРАБОТЧИКИ СООБЩЕНИЙ
# ============================================================================

async def start_handler(message: types.Message):
    """Обработчик /start"""
    user_id = message.from_user.id
    name = message.from_user.first_name or "Гость"
    
    if user_id == TUTOR_ID:
        welcome_text = f"🎓 Добро пожаловать, {name}!\n\nВы авторизованы как репетитор."
    else:
        welcome_text = f"👋 Добро пожаловать, {name}!\n\nВыберите действие:"
    
    await message.answer(
        welcome_text,
        reply_markup=persistent_menu_keyboard()
    )
    
    await message.answer(
        "Выберите действие:",
        reply_markup=main_menu_keyboard(user_id)
    )

async def menu_button_handler(message: types.Message):
    """Обработчик кнопки 'Меню'"""
    user_id = message.from_user.id
    
    await message.answer(
        "📌 Главное меню",
        reply_markup=main_menu_keyboard(user_id)
    )

async def my_schedule_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик нажатия на 'Мое расписание'"""
    user_id = callback.from_user.id
    
    await state.set_state(MyScheduleStates.viewing_schedule)
    
    if user_id == TUTOR_ID:
        lessons = get_tutor_lessons()
        message_text = format_tutor_schedule_message(lessons)
    else:
        lessons = get_student_lessons(user_id)
        message_text = format_student_schedule_message(lessons)
    
    back_btn = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
    ])
    
    await callback.message.edit_text(
        message_text,
        reply_markup=back_btn,
        parse_mode="HTML"
    )
    
    await callback.answer()

async def first_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    """Запуск процесса первого занятия"""
    await callback.message.edit_text(
        "👤 Как вас зовут?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")]
        ])
    )
    
    await state.set_state(FirstLessonStates.waiting_for_name)
    await callback.answer()

async def first_lesson_name_handler(message: types.Message, state: FSMContext):
    """Сохранение имени ученика"""
    name = message.text.strip()
    
    if len(name) < 2:
        await message.answer("❌ Пожалуйста, введите корректное имя (минимум 2 буквы)")
        return
    
    await state.update_data(student_name=name)
    await state.set_state(FirstLessonStates.waiting_for_class)
    
    await message.answer(
        f"📚 Спасибо, {name}! В каком классе вы учитесь?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")]
        ])
    )

async def first_lesson_class_handler(message: types.Message, state: FSMContext):
    """Сохранение класса ученика"""
    class_str = message.text.strip()
    
    if not class_str:
        await message.answer("❌ Пожалуйста, введите класс")
        return
    
    await state.update_data(class_grade=class_str)
    await state.set_state(FirstLessonStates.waiting_for_subject)
    
    await message.answer(
        "📖 Выберите предмет:",
        reply_markup=subjects_keyboard_single()
    )

async def subject_single_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора предмета для первого занятия"""
    subject = callback.data.replace("subject_single_", "")
    
    await state.update_data(subject=subject)
    
    week = get_week_dates()
    
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    days_ru = {
        "Monday": "Понедельник",
        "Tuesday": "Вторник",
        "Wednesday": "Среда",
        "Thursday": "Четверг",
        "Friday": "Пятница",
        "Saturday": "Суббота"
    }
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
        times = get_available_times(day_name, schedule)
        if times:
            date_obj, date_str = week[day_name]
            btn_text = f"{days_ru[day_name]}, {date_str}"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=btn_text, callback_data=f"time_{day_name}")
            ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        "📅 Выберите день:",
        reply_markup=kb
    )
    
    await callback.answer()

async def time_select_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора времени"""
    day_name = callback.data.replace("time_", "")
    
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    times = get_available_times(day_name, schedule)
    
    if not times:
        await callback.answer("❌ На этот день нет доступных времен")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"confirm_time_{day_name}_{time}")] for time in times
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Вернуться", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        "⏰ Выберите время:",
        reply_markup=kb
    )
    
    await callback.answer()

async def confirm_time_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Обработчик подтверждения времени"""
    parts = callback.data.split("_")
    day_name = parts[2]
    time_str = "_".join(parts[3:])
    
    data = await state.get_data()
    student_name = data.get("student_name", "Гость")
    student_class = data.get("class_grade", "")
    subject = data.get("subject", "")
    student_id = callback.from_user.id
    
    lesson_datetime = get_lesson_datetime(day_name, time_str)
    
    if not lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return
    
    # СОХРАНЯЕМ ДАННЫЕ УЧЕНИКА ДЛЯ БУДУЩИХ ЗАНЯТИЙ
    save_student_info(student_id, student_name, student_class)
    
    # Сохраняем запрос в pending
    request_id = create_request_id()
    pending = load_json(PENDING_FILE)
    
    pending[request_id] = {
        "student_id": student_id,
        "student_name": student_name,
        "student_class": student_class,
        "subject": subject,
        "lesson_datetime": lesson_datetime.isoformat(),
        "timestamp": datetime.now().isoformat(),
        "status": "pending"
    }
    
    save_json(PENDING_FILE, pending)
    
    # Уведомляем репетитора
    lesson_date_str = lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = lesson_datetime.strftime("%H:%M")
    
    await bot.send_message(
        TUTOR_ID,
        f"📋 <b>Новый запрос на занятие!</b>\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Дата: {lesson_date_str}\n"
        f"⏰ Время: {lesson_time_str}",
        reply_markup=tutor_confirm_keyboard(request_id),
        parse_mode="HTML"
    )
    
    # Уведомляем ученика
    await callback.message.edit_text(
        f"✅ <b>Запрос отправлен!</b>\n\n"
        f"Репетитор рассмотрит ваш запрос.\n"
        f"Время занятия: <b>{lesson_date_str} {lesson_time_str}</b>\n\n"
        f"Предмет: {subject}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.clear()
    await callback.answer()

async def confirm_request_handler(callback: types.CallbackQuery, bot: Bot):
    """Подтверждение запроса репетитором"""
    request_id = callback.data.replace("confirm_", "")
    
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("❌ Запрос не найден или уже обработан", show_alert=True)
        return
    
    request = pending[request_id]
    student_id = request["student_id"]
    student_name = request["student_name"]
    student_class = request["student_class"]
    subject = request["subject"]
    lesson_datetime_str = request["lesson_datetime"]
    
    # Перемещаем в подтвержденные
    confirmed = load_json(CONFIRMED_FILE)
    lesson_id = create_request_id()
    
    confirmed[lesson_id] = {
        "student_id": student_id,
        "student_name": student_name,
        "student_class": student_class,
        "subject": subject,
        "lesson_datetime": lesson_datetime_str,
        "date_str": datetime.fromisoformat(lesson_datetime_str).strftime("%d.%m.%Y"),
        "time": datetime.fromisoformat(lesson_datetime_str).strftime("%H:%M"),
        "status": "confirmed",
        "timestamp": datetime.now().isoformat()
    }
    
    save_json(CONFIRMED_FILE, confirmed)
    del pending[request_id]
    save_json(PENDING_FILE, pending)
    
    # Уведомляем ученика
    lesson_datetime = datetime.fromisoformat(lesson_datetime_str)
    date_str = lesson_datetime.strftime("%d.%m.%Y")
    time_str = lesson_datetime.strftime("%H:%M")
    
    await bot.send_message(
        student_id,
        f"✅ <b>Ваш запрос подтвержден!</b>\n\n"
        f"📅 Дата: {date_str}\n"
        f"⏰ Время: {time_str}\n"
        f"📖 Предмет: {subject}",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )
    
    # Обновляем сообщение репетитора
    await callback.message.edit_text(
        f"✅ <b>Запрос подтвержден!</b>\n\n"
        f"Ученик {student_name} ({student_class}) был уведомлен.",
        parse_mode="HTML"
    )
    
    await callback.answer("✅ Запрос подтвержден")

async def reject_request_handler(callback: types.CallbackQuery, bot: Bot):
    """Отклонение запроса репетитором"""
    request_id = callback.data.replace("reject_", "")
    
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("❌ Запрос не найден или уже обработан", show_alert=True)
        return
    
    request = pending[request_id]
    student_id = request["student_id"]
    student_name = request["student_name"]
    
    # Удаляем запрос
    del pending[request_id]
    save_json(PENDING_FILE, pending)
    
    # Уведомляем ученика
    await bot.send_message(
        student_id,
        f"❌ <b>Ваш запрос отклонен</b>\n\n"
        f"Репетитор не сможет провести занятие в выбранное время.\n"
        f"Пожалуйста, выберите другое время.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )
    
    # Обновляем сообщение репетитора
    await callback.message.edit_text(
        f"❌ <b>Запрос отклонен!</b>\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )
    
    await callback.answer("❌ Запрос отклонен")

async def repeat_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    """Запуск процесса повторного занятия"""
    lessons = get_student_lessons(callback.from_user.id)
    
    if not lessons:
        await callback.message.edit_text(
            "❌ У вас пока нет забронированных занятий.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()
        return
    
    await state.set_state(RepeatLessonStates.waiting_for_subject)
    
    await callback.message.edit_text(
        "📖 Выберите предмет:",
        reply_markup=subjects_keyboard_single()
    )
    
    await callback.answer()

async def repeat_subject_handler(callback: types.CallbackQuery, state: FSMContext):
    """Выбор предмета для повторного занятия"""
    subject = callback.data.replace("subject_single_", "")
    
    await state.update_data(subject=subject)
    
    week = get_week_dates()
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    days_ru = {
        "Monday": "Понедельник",
        "Tuesday": "Вторник",
        "Wednesday": "Среда",
        "Thursday": "Четверг",
        "Friday": "Пятница",
        "Saturday": "Суббота"
    }
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
        times = get_available_times(day_name, schedule)
        if times:
            date_obj, date_str = week[day_name]
            btn_text = f"{days_ru[day_name]}, {date_str}"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=btn_text, callback_data=f"repeat_time_{day_name}")
            ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        "📅 Выберите день:",
        reply_markup=kb
    )
    
    await state.set_state(RepeatLessonStates.waiting_for_time)
    await callback.answer()

async def repeat_time_select_handler(callback: types.CallbackQuery, state: FSMContext):
    """Выбор времени для повторного занятия"""
    day_name = callback.data.replace("repeat_time_", "")
    
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    times = get_available_times(day_name, schedule)
    
    if not times:
        await callback.answer("❌ На этот день нет доступных времен")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"repeat_confirm_{day_name}_{time}")] for time in times
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Вернуться", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        "⏰ Выберите время:",
        reply_markup=kb
    )
    
    await callback.answer()

async def repeat_confirm_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Подтверждение повторного занятия"""
    parts = callback.data.split("_")
    day_name = parts[2]
    time_str = "_".join(parts[3:])
    
    data = await state.get_data()
    subject = data.get("subject", "")
    student_id = callback.from_user.id
    
    # ПОЛУЧАЕМ СОХРАНЕННЫЕ ДАННЫЕ УЧЕНИКА
    student_info = get_student_info(student_id)
    if not student_info:
        await callback.answer("❌ Ошибка: данные ученика не найдены", show_alert=True)
        return
    
    student_name = student_info["name"]
    student_class = student_info["grade"]
    
    lesson_datetime = get_lesson_datetime(day_name, time_str)
    
    if not lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return
    
    # Сохраняем запрос в pending
    request_id = create_request_id()
    pending = load_json(PENDING_FILE)
    
    pending[request_id] = {
        "student_id": student_id,
        "student_name": student_name,
        "student_class": student_class,
        "subject": subject,
        "lesson_datetime": lesson_datetime.isoformat(),
        "timestamp": datetime.now().isoformat(),
        "status": "pending",
        "type": "repeat"
    }
    
    save_json(PENDING_FILE, pending)
    
    # Уведомляем репетитора
    lesson_date_str = lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = lesson_datetime.strftime("%H:%M")
    
    await bot.send_message(
        TUTOR_ID,
        f"📋 <b>Новый запрос на повторное занятие!</b>\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Дата: {lesson_date_str}\n"
        f"⏰ Время: {lesson_time_str}",
        reply_markup=tutor_confirm_keyboard(request_id),
        parse_mode="HTML"
    )
    
    # Уведомляем ученика
    await callback.message.edit_text(
        f"✅ <b>Запрос отправлен!</b>\n\n"
        f"Репетитор рассмотрит ваш запрос.\n"
        f"Время занятия: <b>{lesson_date_str} {lesson_time_str}</b>\n\n"
        f"Предмет: {subject}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.clear()
    await callback.answer()

async def reschedule_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    """Запуск процесса переноса занятия"""
    lessons = get_student_lessons(callback.from_user.id)
    
    if not lessons:
        await callback.message.edit_text(
            "❌ У вас пока нет забронированных занятий для переноса.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()
        return
    
    await state.set_state(RescheduleStates.choosing_lesson)
    
    await callback.message.edit_text(
        "📅 Выберите занятие для переноса:",
        reply_markup=lessons_list_keyboard(lessons, "reschedule_pick")
    )
    
    await callback.answer()

async def reschedule_pick_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора занятия для переноса"""
    lesson_id = callback.data.replace("reschedule_pick_", "")
    
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        return
    
    lesson = confirmed[lesson_id]
    await state.update_data(reschedule_lesson_id=lesson_id, reschedule_subject=lesson["subject"])
    
    week = get_week_dates()
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    days_ru = {
        "Monday": "Понедельник",
        "Tuesday": "Вторник",
        "Wednesday": "Среда",
        "Thursday": "Четверг",
        "Friday": "Пятница",
        "Saturday": "Суббота"
    }
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
        times = get_available_times(day_name, schedule)
        if times:
            date_obj, date_str = week[day_name]
            btn_text = f"{days_ru[day_name]}, {date_str}"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=btn_text, callback_data=f"reschedule_day_{day_name}")
            ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        "📅 Выберите новый день:",
        reply_markup=kb
    )
    
    await state.set_state(RescheduleStates.waiting_for_new_time)
    await callback.answer()

async def reschedule_day_handler(callback: types.CallbackQuery, state: FSMContext):
    """Выбор дня для переноса"""
    day_name = callback.data.replace("reschedule_day_", "")
    
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    times = get_available_times(day_name, schedule)
    
    if not times:
        await callback.answer("❌ На этот день нет доступных времен")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"reschedule_confirm_{day_name}_{time}")] for time in times
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        "⏰ Выберите новое время:",
        reply_markup=kb
    )
    
    await callback.answer()

async def reschedule_confirm_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Подтверждение переноса занятия"""
    parts = callback.data.split("_")
    day_name = parts[2]
    time_str = "_".join(parts[3:])
    
    data = await state.get_data()
    lesson_id = data.get("reschedule_lesson_id")
    subject = data.get("reschedule_subject")
    student_id = callback.from_user.id
    
    student_info = get_student_info(student_id)
    if not student_info:
        await callback.answer("❌ Ошибка: данные ученика не найдены", show_alert=True)
        return
    
    student_name = student_info["name"]
    student_class = student_info["grade"]
    
    new_lesson_datetime = get_lesson_datetime(day_name, time_str)
    
    if not new_lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return
    
    # Сохраняем запрос на перенос
    reschedule_id = create_request_id()
    pending_reschedules = load_json(PENDING_RESCHEDULES_FILE)
    
    pending_reschedules[reschedule_id] = {
        "lesson_id": lesson_id,
        "student_id": student_id,
        "student_name": student_name,
        "student_class": student_class,
        "subject": subject,
        "new_lesson_datetime": new_lesson_datetime.isoformat(),
        "timestamp": datetime.now().isoformat(),
        "status": "pending"
    }
    
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    # Уведомляем репетитора
    lesson_date_str = new_lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = new_lesson_datetime.strftime("%H:%M")
    
    await bot.send_message(
        TUTOR_ID,
        f"📍 <b>Запрос на перенос занятия!</b>\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Новая дата: {lesson_date_str}\n"
        f"⏰ Новое время: {lesson_time_str}",
        reply_markup=tutor_reschedule_confirm_keyboard(reschedule_id),
        parse_mode="HTML"
    )
    
    # Уведомляем ученика
    await callback.message.edit_text(
        f"✅ <b>Запрос на перенос отправлен!</b>\n\n"
        f"Репетитор рассмотрит ваш запрос.\n"
        f"Новое время: <b>{lesson_date_str} {lesson_time_str}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.clear()
    await callback.answer()

async def confirm_reschedule_handler(callback: types.CallbackQuery, bot: Bot):
    """Подтверждение переноса занятия репетитором"""
    reschedule_id = callback.data.replace("confirm_reschedule_", "")
    
    pending_reschedules = load_json(PENDING_RESCHEDULES_FILE)
    if reschedule_id not in pending_reschedules:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    reschedule = pending_reschedules[reschedule_id]
    lesson_id = reschedule["lesson_id"]
    student_id = reschedule["student_id"]
    student_name = reschedule["student_name"]
    student_class = reschedule["student_class"]
    subject = reschedule["subject"]
    new_datetime_str = reschedule["new_lesson_datetime"]
    
    # Обновляем занятие в confirmed
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id in confirmed:
        confirmed[lesson_id]["lesson_datetime"] = new_datetime_str
        new_datetime = datetime.fromisoformat(new_datetime_str)
        confirmed[lesson_id]["date_str"] = new_datetime.strftime("%d.%m.%Y")
        confirmed[lesson_id]["time"] = new_datetime.strftime("%H:%M")
        save_json(CONFIRMED_FILE, confirmed)
    
    # Удаляем из pending
    del pending_reschedules[reschedule_id]
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    # Уведомляем ученика
    new_datetime = datetime.fromisoformat(new_datetime_str)
    date_str = new_datetime.strftime("%d.%m.%Y")
    time_str = new_datetime.strftime("%H:%M")
    
    await bot.send_message(
        student_id,
        f"✅ <b>Ваш запрос на перенос подтвержден!</b>\n\n"
        f"📅 Новая дата: {date_str}\n"
        f"⏰ Новое время: {time_str}\n"
        f"📖 Предмет: {subject}",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        f"✅ <b>Перенос подтвержден!</b>\n\n"
        f"Ученик {student_name} ({student_class}) был уведомлен.",
        parse_mode="HTML"
    )
    
    await callback.answer("✅ Перенос подтвержден")

async def reject_reschedule_handler(callback: types.CallbackQuery, bot: Bot):
    """Отклонение переноса занятия репетитором"""
    reschedule_id = callback.data.replace("reject_reschedule_", "")
    
    pending_reschedules = load_json(PENDING_RESCHEDULES_FILE)
    if reschedule_id not in pending_reschedules:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    reschedule = pending_reschedules[reschedule_id]
    student_id = reschedule["student_id"]
    student_name = reschedule["student_name"]
    
    del pending_reschedules[reschedule_id]
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    await bot.send_message(
        student_id,
        f"❌ <b>Запрос на перенос отклонен</b>\n\n"
        f"Репетитор не может перенести занятие на это время.\n"
        f"Пожалуйста, выберите другое время.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        f"❌ <b>Перенос отклонен!</b>\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )
    
    await callback.answer("❌ Перенос отклонен")

async def cancel_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    """Запуск процесса отмены занятия"""
    lessons = get_student_lessons(callback.from_user.id)
    
    if not lessons:
        await callback.message.edit_text(
            "❌ У вас пока нет забронированных занятий для отмены.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()
        return
    
    await state.set_state(CancelLessonStates.choosing_lesson)
    
    await callback.message.edit_text(
        "❌ Выберите занятие для отмены:",
        reply_markup=lessons_list_keyboard(lessons, "cancel_pick")
    )
    
    await callback.answer()

async def cancel_pick_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Обработчик выбора занятия для отмены"""
    lesson_id = callback.data.replace("cancel_pick_", "")
    
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        return
    
    lesson = confirmed[lesson_id]
    student_id = callback.from_user.id
    
    student_info = get_student_info(student_id)
    if not student_info:
        await callback.answer("❌ Ошибка: данные ученика не найдены", show_alert=True)
        return
    
    student_name = student_info["name"]
    student_class = student_info["grade"]
    
    # Сохраняем запрос на отмену
    cancel_id = create_request_id()
    pending_cancels = load_json(PENDING_CANCELS_FILE)
    
    pending_cancels[cancel_id] = {
        "lesson_id": lesson_id,
        "student_id": student_id,
        "student_name": student_name,
        "student_class": student_class,
        "subject": lesson["subject"],
        "lesson_datetime": lesson["lesson_datetime"],
        "timestamp": datetime.now().isoformat(),
        "status": "pending"
    }
    
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    # Уведомляем репетитора
    lesson_date_str = lesson["date_str"]
    lesson_time_str = lesson["time"]
    
    await bot.send_message(
        TUTOR_ID,
        f"❌ <b>Запрос на отмену занятия!</b>\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {lesson['subject']}\n"
        f"📅 Дата: {lesson_date_str}\n"
        f"⏰ Время: {lesson_time_str}",
        reply_markup=tutor_cancel_confirm_keyboard(cancel_id),
        parse_mode="HTML"
    )
    
    # Уведомляем ученика
    await callback.message.edit_text(
        f"✅ <b>Запрос на отмену отправлен!</b>\n\n"
        f"Репетитор рассмотрит ваш запрос.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.clear()
    await callback.answer()

async def confirm_cancel_handler(callback: types.CallbackQuery, bot: Bot):
    """Подтверждение отмены занятия репетитором"""
    cancel_id = callback.data.replace("confirm_cancel_", "")
    
    pending_cancels = load_json(PENDING_CANCELS_FILE)
    if cancel_id not in pending_cancels:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    cancel = pending_cancels[cancel_id]
    lesson_id = cancel["lesson_id"]
    student_id = cancel["student_id"]
    student_name = cancel["student_name"]
    
    # Удаляем занятие из confirmed
    confirmed = load_json(CONFIRMED_FILE)
    if lesson_id in confirmed:
        del confirmed[lesson_id]
        save_json(CONFIRMED_FILE, confirmed)
    
    # Удаляем из pending
    del pending_cancels[cancel_id]
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    # Уведомляем ученика
    await bot.send_message(
        student_id,
        f"✅ <b>Ваша заявка на отмену одобрена!</b>\n\n"
        f"Занятие было отменено.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        f"✅ <b>Отмена подтверждена!</b>\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )
    
    await callback.answer("✅ Отмена подтверждена")

async def reject_cancel_handler(callback: types.CallbackQuery, bot: Bot):
    """Отклонение отмены занятия репетитором"""
    cancel_id = callback.data.replace("reject_cancel_", "")
    
    pending_cancels = load_json(PENDING_CANCELS_FILE)
    if cancel_id not in pending_cancels:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    cancel = pending_cancels[cancel_id]
    student_id = cancel["student_id"]
    student_name = cancel["student_name"]
    
    del pending_cancels[cancel_id]
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    await bot.send_message(
        student_id,
        f"❌ <b>Запрос на отмену отклонен</b>\n\n"
        f"Репетитор не отменяет это занятие.\n"
        f"Занятие остается в расписании.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        f"❌ <b>Отмена отклонена!</b>\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )
    
    await callback.answer("❌ Отмена отклонена")

async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    
    user_id = callback.from_user.id
    
    await callback.message.edit_text(
        "📌 Главное меню",
        reply_markup=main_menu_keyboard(user_id)
    )
    
    await callback.answer()

async def edit_schedule_button_handler(callback: types.CallbackQuery, state: FSMContext):
    """Запуск интерактивного редактирования расписания"""
    
    current_schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    
    await state.update_data(
        interactive_schedule=current_schedule.copy(),
        edited_days=[]
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Пн", callback_data="iday_Monday"),
            InlineKeyboardButton(text="Вт", callback_data="iday_Tuesday"),
            InlineKeyboardButton(text="Ср", callback_data="iday_Wednesday"),
        ],
        [
            InlineKeyboardButton(text="Чт", callback_data="iday_Thursday"),
            InlineKeyboardButton(text="Пт", callback_data="iday_Friday"),
            InlineKeyboardButton(text="Сб", callback_data="iday_Saturday"),
        ],
        [
            InlineKeyboardButton(text="✅ Сохранить расписание", callback_data="isave_schedule"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_menu"),
        ]
    ])
    
    await callback.message.edit_text(
        "🛠 <b>Интерактивное редактирование расписания</b>\n\n"
        "Выберите день недели для редактирования:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.choosing_day)
    await callback.answer()


async def interactive_day_select_handler(callback: types.CallbackQuery, state: FSMContext):
    """Выбор дня для редактирования"""
    
    day_name = callback.data.replace("iday_", "")
    day_ru = DAYS_RU.get(day_name, day_name)
    
    await state.update_data(current_day=day_name)
    
    await callback.message.edit_text(
        f"📅 <b>{day_ru}</b>\n\n"
        f"Когда вы можете начать занятия в {day_ru}?\n\n"
        "<code>Примеры:</code>\n"
        "• <code>19:30</code> — начало в 19:30\n"
        "• <code>18</code> — начало в 18:00\n"
        "• <code>нет</code> — нет занятий в этот день\n\n"
        f"<i>Бот автоматически создаст слоты по 1 часу (до {MAX_WORK_HOUR}:00)</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_schedule_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.waiting_for_start_time)
    await callback.answer()


async def interactive_time_input_handler(message: types.Message, state: FSMContext):
    """Обработка ввода времени ДЛЯ РАСПИСАНИЯ (ТОЛЬКО)"""
    
    current_state = await state.get_state()
    if current_state != InteractiveScheduleStates.waiting_for_start_time:
        return
    
    data = await state.get_data()
    day_name = data.get("current_day")
    day_ru = DAYS_RU.get(day_name, day_name)
    interactive_schedule = data.get("interactive_schedule", {})
    
    if not day_name:
        await message.answer("❌ Ошибка: день не выбран")
        return
    
    time_input = parse_time_input(message.text)
    
    if time_input == "invalid":
        await message.answer(
            "❌ Неверный формат времени.\n"
            "Используйте: 19:30 или 19 или нет"
        )
        return
    
    if time_input is None:
        interactive_schedule[day_name] = []
        message_text = f"✅ <b>{day_ru}:</b> нет занятий"
    else:
        start_h, start_m = time_input
        slots = generate_time_slots(start_h, start_m)
        interactive_schedule[day_name] = slots
        slots_str = ", ".join(slots)
        message_text = f"✅ <b>{day_ru}:</b>\n{slots_str}\n\n(автоматически созданы слоты по 1 часу)"
    
    await state.update_data(interactive_schedule=interactive_schedule)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Пн", callback_data="iday_Monday"),
            InlineKeyboardButton(text="Вт", callback_data="iday_Tuesday"),
            InlineKeyboardButton(text="Ср", callback_data="iday_Wednesday"),
        ],
        [
            InlineKeyboardButton(text="Чт", callback_data="iday_Thursday"),
            InlineKeyboardButton(text="Пт", callback_data="iday_Friday"),
            InlineKeyboardButton(text="Сб", callback_data="iday_Saturday"),
        ],
        [
            InlineKeyboardButton(text="✅ Сохранить расписание", callback_data="isave_schedule"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_menu"),
        ]
    ])
    
    await message.answer(
        message_text + "\n\n" + format_schedule_for_preview(interactive_schedule),
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.choosing_day)


async def interactive_save_handler(callback: types.CallbackQuery, state: FSMContext):
    """Сохранение расписания"""
    
    data = await state.get_data()
    interactive_schedule = data.get("interactive_schedule", {})
    
    save_json(SCHEDULE_FILE, interactive_schedule)
    
    preview = format_schedule_for_preview(interactive_schedule)
    
    await callback.message.edit_text(
        "✅ <b>Расписание успешно обновлено!</b>\n\n" + preview,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.clear()
    await callback.answer()


async def back_to_schedule_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в меню редактирования"""
    
    data = await state.get_data()
    interactive_schedule = data.get("interactive_schedule", {})
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Пн", callback_data="iday_Monday"),
            InlineKeyboardButton(text="Вт", callback_data="iday_Tuesday"),
            InlineKeyboardButton(text="Ср", callback_data="iday_Wednesday"),
        ],
        [
            InlineKeyboardButton(text="Чт", callback_data="iday_Thursday"),
            InlineKeyboardButton(text="Пт", callback_data="iday_Friday"),
            InlineKeyboardButton(text="Сб", callback_data="iday_Saturday"),
        ],
        [
            InlineKeyboardButton(text="✅ Сохранить расписание", callback_data="isave_schedule"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_menu"),
        ]
    ])
    
    await callback.message.edit_text(
        "🛠 <b>Интерактивное редактирование расписания</b>\n\n"
        "Выберите день недели для редактирования:\n\n"
        + format_schedule_for_preview(interactive_schedule),
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.choosing_day)
    await callback.answer()

# ============================================================================
# ЗАДАЧИ
# ============================================================================

async def send_reminders(bot: Bot):
    """Отправка напоминаний о предстоящих занятиях"""
    await asyncio.sleep(60)
    
    while True:
        try:
            now = datetime.now()
            confirmed = load_json(CONFIRMED_FILE)
            
            for lesson_id, lesson in confirmed.items():
                try:
                    lesson_time = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
                    time_diff = (lesson_time - now).total_seconds()
                    
                    if 600 <= time_diff <= 900:
                        student_id = lesson.get("student_id")
                        student_name = lesson.get("student_name", "Гость")
                        subject = lesson.get("subject", "")
                        lesson_time_str = lesson_time.strftime("%H:%M")
                        
                        await bot.send_message(
                            student_id,
                            f"⏰ <b>Напоминание!</b>\n\n"
                            f"Через 15 минут начнется занятие по {subject}.\n"
                            f"Время: {lesson_time_str}",
                            parse_mode="HTML",
                            reply_markup=persistent_menu_keyboard()
                        )
                        
                        await bot.send_message(
                            TUTOR_ID,
                            f"⏰ <b>Напоминание!</b>\n\n"
                            f"Через 15 минут занятие с {student_name} по {subject}.\n"
                            f"Время: {lesson_time_str}",
                            parse_mode="HTML"
                        )
                except:
                    pass
            
            await asyncio.sleep(300)
        except Exception as e:
            print(f"⚠️ Ошибка в send_reminders: {e}")
            await asyncio.sleep(60)

async def send_daily_schedule(bot: Bot):
    """Отправка расписания в начале дня"""
    await asyncio.sleep(120)
    
    while True:
        try:
            now = datetime.now()
            
            if now.hour == 8 and now.minute < 1:
                lessons = get_tutor_lessons()
                message = format_tutor_schedule_message(lessons)
                
                await bot.send_message(
                    TUTOR_ID,
                    message,
                    parse_mode="HTML",
                    reply_markup=persistent_menu_keyboard()
                )
            
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"⚠️ Ошибка в send_daily_schedule: {e}")
            await asyncio.sleep(60)

async def cleanup_task(bot: Bot):
    """Периодическая очистка старых запросов"""
    await asyncio.sleep(300)
    
    while True:
        try:
            cleanup_stale_requests()
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"⚠️ Ошибка в cleanup_task: {e}")
            await asyncio.sleep(60)

async def keep_alive_task():
    """Keep-alive для предотвращения гибернации на Render"""
    if not RENDER_URL:
        return
    
    await asyncio.sleep(30)
    
    while True:
        try:
            await asyncio.sleep(840)
            async with ClientSession() as session:
                try:
                    async with session.get(f"{RENDER_URL}/health", timeout=5) as resp:
                        if resp.status == 200:
                            print(f"✅ Keep-alive ping успешен [{datetime.now().strftime('%H:%M:%S')}]")
                except Exception as e:
                    print(f"⚠️ Keep-alive ошибка: {e}")
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
            dp.message.register(interactive_time_input_handler, InteractiveScheduleStates.waiting_for_start_time)
            
            dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")
            dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")
            dp.callback_query.register(reschedule_lesson_handler, F.data == "reschedule_lesson")
            dp.callback_query.register(cancel_lesson_handler, F.data == "cancel_lesson")
            dp.callback_query.register(my_schedule_handler, F.data == "my_schedule")
            dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")
            dp.callback_query.register(subject_single_handler, F.data.startswith("subject_single_"))
            dp.callback_query.register(repeat_subject_handler, F.data.startswith("subject_single_"), RepeatLessonStates.waiting_for_subject)
            dp.callback_query.register(repeat_time_select_handler, F.data.startswith("repeat_time_"))
            dp.callback_query.register(repeat_confirm_handler, F.data.startswith("repeat_confirm_"))
            dp.callback_query.register(time_select_handler, F.data.startswith("time_"))
            dp.callback_query.register(confirm_time_handler, F.data.startswith("confirm_time_"))
            dp.callback_query.register(reschedule_pick_handler, F.data.startswith("reschedule_pick_"))
            dp.callback_query.register(reschedule_day_handler, F.data.startswith("reschedule_day_"))
            dp.callback_query.register(reschedule_confirm_handler, F.data.startswith("reschedule_confirm_"))
            dp.callback_query.register(cancel_pick_handler, F.data.startswith("cancel_pick_"))
            dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
            dp.callback_query.register(interactive_day_select_handler, F.data.startswith("iday_"))
            dp.callback_query.register(interactive_save_handler, F.data == "isave_schedule")
            dp.callback_query.register(back_to_schedule_menu_handler, F.data == "back_to_schedule_menu")
            dp.callback_query.register(confirm_reschedule_handler, F.data.startswith("confirm_reschedule_"))
            dp.callback_query.register(reject_reschedule_handler, F.data.startswith("reject_reschedule_"))
            dp.callback_query.register(confirm_cancel_handler, F.data.startswith("confirm_cancel_"))
            dp.callback_query.register(reject_cancel_handler, F.data.startswith("reject_cancel_"))
            dp.callback_query.register(confirm_request_handler, F.data.startswith("confirm_"))
            dp.callback_query.register(reject_request_handler, F.data.startswith("reject_"))
            
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
                print(f"\n⚠️ TelegramConflictError! Попытка перезапуска {retry_count}/{max_retries}")
                print(f" Ожидаю {wait_time} секунд перед перезапуском...")
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
        sys.exit(1)

# ============================================================================
# MAIN
# ============================================================================

async def main():
    print("=" * 70)
    print("INITIALIZING APPLICATION - COMPLETE SYSTEM (FULLY FIXED)")
    print("=" * 70)
    print(f"Port: {PORT}")
    print(f"Token: {'OK' if TOKEN else 'NOT SET'}")
    print(f"Render URL: {RENDER_URL if RENDER_URL else 'NOT SET'}")
    print(f"Max work hour: {MAX_WORK_HOUR}:00")
    print(f"Slot duration: {SLOT_DURATION} минут")
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
        print("\n⏹️ Application interrupted by user")
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

