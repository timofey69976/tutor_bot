# -*- coding: utf-8 -*-

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
     TOKEN = '7954650918:AAHF3GJRZKbp3ihoaWN6UeYAsxHRY-A4V3w'

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

SLOT_DURATION = 60
MAX_WORK_HOUR = 21
MAX_WORK_MINUTE = 0
REMINDER_TIME_MINUTES = 60  # ✅ ИСПРАВЛЕНО: напоминание за ЧАС (60 минут)

DAYS_RU = {
    "Monday": "Понедельник",
    "Tuesday": "Вторник",
    "Wednesday": "Среда",
    "Thursday": "Четверг",
    "Friday": "Пятница",
    "Saturday": "Суббота"
}

DATA_DIR = Path("bot_data")
DATA_DIR.mkdir(exist_ok=True)
STUDENTS_FILE = DATA_DIR / "students.json"
SCHEDULE_FILE = DATA_DIR / "schedule.json"
PENDING_FILE = DATA_DIR / "pending_requests.json"
CONFIRMED_FILE = DATA_DIR / "confirmed_lessons.json"
PENDING_RESCHEDULES_FILE = DATA_DIR / "pending_reschedules.json"
PENDING_CANCELS_FILE = DATA_DIR / "pending_cancels.json"
REMINDERS_SENT_FILE = DATA_DIR / "reminders_sent.json"  # ✅ НОВОЕ: отслеживание отправленных напоминаний

# ✅ НОВОЕ: Глобальный кеш студентов в памяти
STUDENT_CACHE = {}

def load_json(filepath):
    """Безопасная загрузка JSON файла"""
    try:
        if filepath.exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ Ошибка при загрузке {filepath}: {e}")
    return {}

def save_json(filepath, data):
    """Безопасное сохранение JSON файла"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ Ошибка при сохранении {filepath}: {e}")

def cleanup_stale_requests():
    """Удаление старых запросов старше 24 часов"""
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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s, callback_data=f"subject_single_{s}")] for s in SUBJECTS
    ])

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
# ФУНКЦИИ УПРАВЛЕНИЯ ДАННЫМИ СТУДЕНТОВ
# ============================================================================

def cache_student_info(student_id: int, name: str, grade: str):
    """Кешировать данные в памяти + сохранить в файл"""
    STUDENT_CACHE[student_id] = {"name": name, "grade": grade}
    students = load_json(STUDENTS_FILE)
    students[str(student_id)] = {"name": name, "grade": grade}
    save_json(STUDENTS_FILE, students)
    print(f"✅ Кешировано и сохранено: {name} ({grade}) - ID: {student_id}")

def get_student_info_from_any_source(student_id: int) -> Optional[Dict]:
    """
    Получить данные ученика из ЛЮБОГО источника по приоритету:
    1. Кеш в памяти
    2. students.json файл
    3. confirmed lessons (уже подтвержденные занятия)
    4. pending requests (ожидающие подтверждения)
    """
    # Вариант 1: Кеш в памяти
    if student_id in STUDENT_CACHE:
        info = STUDENT_CACHE[student_id]
        print(f"✅ Найдено в памяти: {info['name']} ({info['grade']}) - ID: {student_id}")
        return info

    # Вариант 2: students.json файл
    students = load_json(STUDENTS_FILE)
    if str(student_id) in students:
        info = students[str(student_id)]
        STUDENT_CACHE[student_id] = info  # Добавить в кеш
        print(f"✅ Найдено в students.json: {info['name']} ({info['grade']}) - ID: {student_id}")
        return info

    # Вариант 3: Из confirmed lessons (уже подтвержденные)
    confirmed = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed.items():
        if lesson.get("student_id") == student_id:
            name = lesson.get("student_name", "")
            grade = lesson.get("student_class", "")
            if name and grade:
                info = {"name": name, "grade": grade}
                cache_student_info(student_id, name, grade)  # Сохранить для будущего
                print(f"✅ Восстановлено из confirmed lessons: {name} ({grade}) - ID: {student_id}")
                return info

    # Вариант 4: Из pending requests (ожидающие подтверждения)
    pending = load_json(PENDING_FILE)
    for req_id, req in pending.items():
        if req.get("student_id") == student_id:
            name = req.get("student_name", "")
            grade = req.get("student_class", "")
            if name and grade:
                info = {"name": name, "grade": grade}
                cache_student_info(student_id, name, grade)  # Сохранить для будущего
                print(f"✅ Восстановлено из pending requests: {name} ({grade}) - ID: {student_id}")
                return info

    print(f"❌ Информация ученика не найдена: ID: {student_id}")
    return None

def get_student_info(student_id: int) -> Optional[Dict]:
    """Получить данные ученика"""
    return get_student_info_from_any_source(student_id)

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def get_week_dates(start_date: datetime = None) -> Dict:
    """Получить даты текущей недели (понедельник - суббота)"""
    if start_date is None:
        start_date = datetime.now()

    current_weekday = start_date.weekday()
    if current_weekday == 6:  # Sunday
        week_start = start_date + timedelta(days=1)
    else:
        days_back = current_weekday
        week_start = start_date - timedelta(days=days_back)

    days_map = {0: "Monday", 1: "Tuesday", 2: "Wednesday",
                3: "Thursday", 4: "Friday", 5: "Saturday"}
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

def get_booked_times() -> Dict[str, bool]:
    """Получить все забронированные времена по датам и времени (ключ: YYYY-MM-DD_HH:MM)"""
    booked: Dict[str, bool] = {}
    confirmed = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed.items():
        try:
            lesson_datetime = datetime.fromisoformat(lesson["lesson_datetime"])
            lesson_time = lesson_datetime.strftime("%H:%M")
            date_str = lesson_datetime.strftime("%Y-%m-%d")
            key = f"{date_str}_{lesson_time}"
            booked[key] = True
        except Exception as e:
            print(f"⚠️ Ошибка при обработке confirmed lesson {lesson_id}: {e}")
            continue

    return booked

def is_time_slot_booked(day_name: str, time_str: str) -> bool:
    """Проверить, занято ли время на конкретный день текущей недели"""
    week = get_week_dates()
    if day_name not in week:
        return True

    date_obj, _ = week[day_name]
    date_str = date_obj.strftime("%Y-%m-%d")
    key = f"{date_str}_{time_str}"

    booked = get_booked_times()
    return key in booked

def get_available_times(day_name: str, schedule: Dict) -> List[str]:
    """Получить доступные времена для дня (исключая забронированные)"""
    all_times = schedule.get(day_name, [])
    if not all_times:
        return []

    available = [time for time in all_times if not is_time_slot_booked(day_name, time)]
    return available

def create_request_id():
    """Создать уникальный ID запроса"""
    import uuid
    return str(uuid.uuid4())[:8]

def parse_time(time_str: str) -> tuple:
    """Парсить время из строки HH:MM"""
    parts = time_str.split(":")
    return int(parts[0]), int(parts[1])

def get_lesson_datetime(day_name: str, time_str: str) -> Optional[datetime]:
    """Получить datetime для занятия"""
    week = get_week_dates()
    if day_name not in week:
        return None

    date_obj, _ = week[day_name]
    hour, minute = parse_time(time_str)
    return date_obj.replace(hour=hour, minute=minute, second=0, microsecond=0)

def get_student_lessons(student_id: int) -> Dict:
    """Получить подтвержденные занятия ученика"""
    confirmed = load_json(CONFIRMED_FILE)
    return {lid: l for lid, l in confirmed.items() if l["student_id"] == student_id}

def get_tutor_lessons() -> Dict:
    """Получить занятия репетитора на эту неделю"""
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

def format_student_schedule_message(lessons: Dict) -> str:
    """Форматировать расписание ученика"""
    if not lessons:
        return "📭 У вас нет занятий на эту неделю."

    message = "📚 Ваше расписание на эту неделю:\n\n"
    sorted_lessons = sorted(lessons.values(), key=lambda x: x.get("lesson_datetime", ""))

    for lesson in sorted_lessons:
        try:
            lesson_date = datetime.fromisoformat(lesson["lesson_datetime"])
            date_str = lesson_date.strftime("%d.%m.%Y")
            time_str = lesson_date.strftime("%H:%M")
            subject = lesson.get("subject", "Неизвестный предмет")
            message += f"📅 {date_str} в {time_str}\n"
            message += f" Предмет: {subject}\n"
            message += f" Статус: ✅ Подтверждено\n\n"
        except:
            pass

    return message

def format_tutor_schedule_message(lessons: Dict) -> str:
    """Форматировать расписание репетитора"""
    if not lessons:
        return "📭 У вас нет занятий на эту неделю."

    message = "📚 Ваше расписание на эту неделю:\n\n"
    sorted_lessons = sorted(lessons.values(), key=lambda x: x.get("lesson_datetime", ""))

    for lesson in sorted_lessons:
        try:
            lesson_date = datetime.fromisoformat(lesson["lesson_datetime"])
            date_str = lesson_date.strftime("%d.%m.%Y")
            time_str = lesson_date.strftime("%H:%M")
            student_name = lesson.get("student_name", "Неизвестный ученик")
            subject = lesson.get("subject", "Неизвестный предмет")
            message += f"📅 {date_str} в {time_str}\n"
            message += f" Ученик: {student_name}\n"
            message += f" Предмет: {subject}\n"
            message += f" Статус: ✅ Подтверждено\n\n"
        except:
            pass

    return message

def parse_time_input(text: str) -> Optional[Tuple[int, int]]:
    """Парсить ввод времени от пользователя"""
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
    """Сгенерировать слоты времени"""
    slots = []
    current_hour = start_hour
    current_minute = start_minute

    max_minutes = MAX_WORK_HOUR * 60 + MAX_WORK_MINUTE

    while True:
        current_minutes = current_hour * 60 + current_minute

        if current_minutes > max_minutes or current_hour >= 24:
            break

        time_str = f"{current_hour:02d}:{current_minute:02d}"
        slots.append(time_str)

        current_minute += SLOT_DURATION
        if current_minute >= 60:
            current_hour += current_minute // 60
            current_minute = current_minute % 60

    return slots

def format_schedule_for_preview(schedule_dict: Dict) -> str:
    """Форматировать расписание для превью"""
    message = "📋 Ваше расписание:\n\n"

    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
        day_ru = DAYS_RU.get(day_name, day_name)
        times = schedule_dict.get(day_name, [])

        if isinstance(times, str) and times == "нет":
            times_str = "❌ нет занятий"
        elif times:
            times_str = ", ".join(times)
        else:
            times_str = "⏳ не установлено"

        message += f"📅 {day_ru}: {times_str}\n"

    return message

# ============================================================================
# ОБРАБОТЧИКИ СООБЩЕНИЙ
# ============================================================================

async def start_handler(message: types.Message):
    user_id = message.from_user.id
    name = message.from_user.first_name or "Гость"

    if user_id == TUTOR_ID:
        welcome_text = f"🎓 Добро пожаловать, {name}!\n\nВы авторизованы как репетитор."
    else:
        welcome_text = f"👋 Добро пожаловать, {name}!\n\nВыберите действие:"

    await message.answer(welcome_text, reply_markup=persistent_menu_keyboard())
    await message.answer("Выберите действие:", reply_markup=main_menu_keyboard(user_id))

async def menu_button_handler(message: types.Message):
    user_id = message.from_user.id
    await message.answer("📌 Главное меню", reply_markup=main_menu_keyboard(user_id))

async def my_schedule_handler(callback: types.CallbackQuery, state: FSMContext):
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

    await callback.message.edit_text(message_text, reply_markup=back_btn, parse_mode="HTML")
    await callback.answer()

async def first_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "👤 Как вас зовут?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")]
        ])
    )
    await state.set_state(FirstLessonStates.waiting_for_name)
    await callback.answer()

async def first_lesson_name_handler(message: types.Message, state: FSMContext):
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
    class_str = message.text.strip()
    if not class_str:
        await message.answer("❌ Пожалуйста, введите класс")
        return

    await state.update_data(class_grade=class_str)
    await state.set_state(FirstLessonStates.waiting_for_subject)
    await message.answer("📖 Выберите предмет:", reply_markup=subjects_keyboard_single())

async def subject_single_handler(callback: types.CallbackQuery, state: FSMContext):
    subject = callback.data.replace("subject_single_", "")
    current_state = await state.get_state()
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
            if current_state == FirstLessonStates.waiting_for_subject:
                kb.inline_keyboard.append([
                    InlineKeyboardButton(text=btn_text, callback_data=f"time_{day_name}")
                ])
            elif current_state == RepeatLessonStates.waiting_for_subject:
                kb.inline_keyboard.append([
                    InlineKeyboardButton(text=btn_text, callback_data=f"repeat_time_{day_name}")
                ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")
    ])

    if current_state == FirstLessonStates.waiting_for_subject:
        await state.set_state(FirstLessonStates.waiting_for_time)
    elif current_state == RepeatLessonStates.waiting_for_subject:
        await state.set_state(RepeatLessonStates.waiting_for_time)

    await callback.message.edit_text("📅 Выберите день:", reply_markup=kb)
    await callback.answer()

async def time_select_handler(callback: types.CallbackQuery, state: FSMContext):
    day_name = callback.data.replace("time_", "")
    schedule = load_json(SCHEDULE_FILE)

    if not schedule:
        schedule = DEFAULT_SCHEDULE

    times = get_available_times(day_name, schedule)

    if not times:
        await callback.answer("❌ На этот день нет доступных свободных времен")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"confirm_time_{day_name}_{time}")] for time in times
    ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Вернуться", callback_data="back_to_menu")
    ])

    await callback.message.edit_text("⏰ Выберите время:", reply_markup=kb)
    await callback.answer()

async def confirm_time_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split("_")
    day_name = parts[2]
    time_str = "_".join(parts[3:])

    data = await state.get_data()
    student_name = data.get("student_name", "Гость")
    student_class = data.get("class_grade", "")
    subject = data.get("subject", "")
    student_id = callback.from_user.id

    # Финальная проверка: вдруг слот уже успели занять
    if is_time_slot_booked(day_name, time_str):
        await callback.answer("❌ Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return

    lesson_datetime = get_lesson_datetime(day_name, time_str)

    if not lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return

    # ✅ Сохраняем данные ученика СРАЗУ в кеш перед отправкой запроса
    cache_student_info(student_id, student_name, student_class)

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
    print(f"📝 Создан запрос на занятие: {request_id} - {student_name} ({student_class})")

    lesson_date_str = lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = lesson_datetime.strftime("%H:%M")

    await bot.send_message(
        TUTOR_ID,
        f"📋 Новый запрос на занятие!\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Дата: {lesson_date_str}\n"
        f"⏰ Время: {lesson_time_str}",
        reply_markup=tutor_confirm_keyboard(request_id),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Запрос отправлен!\n\n"
        f"Репетитор рассмотрит ваш запрос.\n"
        f"Время занятия: {lesson_date_str} {lesson_time_str}\n\n"
        f"Предмет: {subject}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )

    await state.clear()
    await callback.answer()

async def confirm_request_handler(callback: types.CallbackQuery, bot: Bot):
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

    # Сохраняем данные ученика
    cache_student_info(student_id, student_name, student_class)

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

    print(f"✅ Занятие подтверждено: {lesson_id} - {student_name}")

    lesson_datetime = datetime.fromisoformat(lesson_datetime_str)
    date_str = lesson_datetime.strftime("%d.%m.%Y")
    time_str = lesson_datetime.strftime("%H:%M")

    await bot.send_message(
        student_id,
        f"✅ Ваш запрос подтвержден!\n\n"
        f"📅 Дата: {date_str}\n"
        f"⏰ Время: {time_str}\n"
        f"📖 Предмет: {subject}",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Запрос подтвержден!\n\n"
        f"Ученик {student_name} ({student_class}) был уведомлен.",
        parse_mode="HTML"
    )

    await callback.answer("✅ Запрос подтвержден")

async def reject_request_handler(callback: types.CallbackQuery, bot: Bot):
    request_id = callback.data.replace("reject_", "")
    pending = load_json(PENDING_FILE)

    if request_id not in pending:
        await callback.answer("❌ Запрос не найден или уже обработан", show_alert=True)
        return

    request = pending[request_id]

    student_id = request["student_id"]
    student_name = request["student_name"]

    del pending[request_id]
    save_json(PENDING_FILE, pending)

    print(f"❌ Запрос отклонен: {request_id} - {student_name}")

    await bot.send_message(
        student_id,
        f"❌ Ваш запрос отклонен\n\n"
        f"Репетитор не сможет провести занятие в выбранное время.\n"
        f"Пожалуйста, выберите другое время.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"❌ Запрос отклонен!\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )

    await callback.answer("❌ Запрос отклонен")

async def repeat_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
    student_id = callback.from_user.id
    lessons = get_student_lessons(student_id)

    if not lessons:
        await callback.message.edit_text(
            "❌ У вас пока нет забронированных занятий.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()
        return

    # Восстанавливаем данные ученика
    student_info = get_student_info_from_any_source(student_id)

    if student_info:
        await state.update_data(
            student_name=student_info["name"],
            class_grade=student_info["grade"]
        )

    await state.set_state(RepeatLessonStates.waiting_for_subject)
    await callback.message.edit_text("📖 Выберите предмет:", reply_markup=subjects_keyboard_single())
    await callback.answer()

async def repeat_time_select_handler(callback: types.CallbackQuery, state: FSMContext):
    day_name = callback.data.replace("repeat_time_", "")
    schedule = load_json(SCHEDULE_FILE)

    if not schedule:
        schedule = DEFAULT_SCHEDULE

    times = get_available_times(day_name, schedule)

    if not times:
        await callback.answer("❌ На этот день нет доступных свободных времен")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"repeat_confirm_{day_name}_{time}")] for time in times
    ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Вернуться", callback_data="back_to_menu")
    ])

    await callback.message.edit_text("⏰ Выберите время:", reply_markup=kb)
    await callback.answer()

async def repeat_confirm_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split("_")
    day_name = parts[2]
    time_str = "_".join(parts[3:])

    data = await state.get_data()
    subject = data.get("subject", "")
    student_id = callback.from_user.id

    # Восстанавливаем данные ученика
    student_info = get_student_info_from_any_source(student_id)

    if not student_info:
        await callback.answer("❌ Ошибка: данные ученика не найдены. Пожалуйста, сначала запишитесь на первое занятие!", show_alert=True)
        return

    # Проверка занятости слота
    if is_time_slot_booked(day_name, time_str):
        await callback.answer("❌ Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return

    student_name = student_info["name"]
    student_class = student_info["grade"]

    print(f"✅ Загружены данные для повторного занятия: {student_name} ({student_class}) - ID: {student_id}")

    lesson_datetime = get_lesson_datetime(day_name, time_str)

    if not lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return

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

    print(f"📝 Создан запрос на повторное занятие: {request_id} - {student_name} ({student_class})")

    lesson_date_str = lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = lesson_datetime.strftime("%H:%M")

    await bot.send_message(
        TUTOR_ID,
        f"📋 Новый запрос на повторное занятие!\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Дата: {lesson_date_str}\n"
        f"⏰ Время: {lesson_time_str}",
        reply_markup=tutor_confirm_keyboard(request_id),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Запрос отправлен!\n\n"
        f"Репетитор рассмотрит ваш запрос.\n"
        f"Время занятия: {lesson_date_str} {lesson_time_str}\n\n"
        f"Предмет: {subject}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )

    await state.clear()
    await callback.answer()

async def reschedule_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
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
    await callback.message.edit_text("📅 Выберите занятие для переноса:", reply_markup=lessons_list_keyboard(lessons, "reschedule_pick"))
    await callback.answer()

async def reschedule_pick_handler(callback: types.CallbackQuery, state: FSMContext):
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

    await callback.message.edit_text("📅 Выберите новый день:", reply_markup=kb)
    await state.set_state(RescheduleStates.waiting_for_new_time)
    await callback.answer()

async def reschedule_day_handler(callback: types.CallbackQuery, state: FSMContext):
    day_name = callback.data.replace("reschedule_day_", "")
    schedule = load_json(SCHEDULE_FILE)

    if not schedule:
        schedule = DEFAULT_SCHEDULE

    times = get_available_times(day_name, schedule)

    if not times:
        await callback.answer("❌ На этот день нет доступных свободных времен")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"reschedule_confirm_{day_name}_{time}")] for time in times
    ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])

    await callback.message.edit_text("⏰ Выберите новое время:", reply_markup=kb)
    await callback.answer()

async def reschedule_confirm_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    parts = callback.data.split("_")
    day_name = parts[2]
    time_str = "_".join(parts[3:])

    data = await state.get_data()
    lesson_id = data.get("reschedule_lesson_id")
    subject = data.get("reschedule_subject")
    student_id = callback.from_user.id

    # Проверяем, что слот не занят
    if is_time_slot_booked(day_name, time_str):
        await callback.answer("❌ Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return

    # Восстанавливаем данные ученика
    student_info = get_student_info_from_any_source(student_id)

    if not student_info:
        confirmed = load_json(CONFIRMED_FILE)
        lesson = confirmed.get(lesson_id, {})
        student_name = lesson.get("student_name", "Ученик")
        student_class = lesson.get("student_class", "")
        print(f"⚠️ ВНИМАНИЕ: данные {student_id} восстановлены из lessons: {student_name} ({student_class})")
        cache_student_info(student_id, student_name, student_class)
    else:
        student_name = student_info["name"]
        student_class = student_info["grade"]
        print(f"✅ Загружены данные для переноса: {student_name} ({student_class}) - ID: {student_id}")

    new_lesson_datetime = get_lesson_datetime(day_name, time_str)

    if not new_lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return

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

    print(f"📝 Создан запрос на перенос занятия: {reschedule_id} - {student_name} ({student_class})")

    lesson_date_str = new_lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = new_lesson_datetime.strftime("%H:%M")

    await bot.send_message(
        TUTOR_ID,
        f"📍 Запрос на перенос занятия!\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Новая дата: {lesson_date_str}\n"
        f"⏰ Новое время: {lesson_time_str}",
        reply_markup=tutor_reschedule_confirm_keyboard(reschedule_id),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Запрос на перенос отправлен!\n\n"
        f"Репетитор рассмотрит ваш запрос.\n"
        f"Новое время: {lesson_date_str} {lesson_time_str}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )

    await state.clear()
    await callback.answer()

async def confirm_reschedule_handler(callback: types.CallbackQuery, bot: Bot):
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

    # Убеждаемся, что данные сохранены
    cache_student_info(student_id, student_name, student_class)

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id in confirmed:
        confirmed[lesson_id]["lesson_datetime"] = new_datetime_str

        new_datetime = datetime.fromisoformat(new_datetime_str)
        confirmed[lesson_id]["date_str"] = new_datetime.strftime("%d.%m.%Y")
        confirmed[lesson_id]["time"] = new_datetime.strftime("%H:%M")

        save_json(CONFIRMED_FILE, confirmed)

    del pending_reschedules[reschedule_id]
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)

    print(f"✅ Перенос занятия подтвержден: {reschedule_id} - {student_name} ({student_class})")

    new_datetime = datetime.fromisoformat(new_datetime_str)
    date_str = new_datetime.strftime("%d.%m.%Y")
    time_str = new_datetime.strftime("%H:%M")

    await bot.send_message(
        student_id,
        f"✅ Ваш запрос на перенос подтвержден!\n\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Новая дата: {date_str}\n"
        f"⏰ Новое время: {time_str}",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Перенос подтвержден!\n\n"
        f"Ученик {student_name} ({student_class}) был уведомлен.",
        parse_mode="HTML"
    )

    await callback.answer("✅ Перенос подтвержден")

async def reject_reschedule_handler(callback: types.CallbackQuery, bot: Bot):
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

    print(f"❌ Перенос занятия отклонен: {reschedule_id} - {student_name}")

    await bot.send_message(
        student_id,
        f"❌ Запрос на перенос отклонен\n\n"
        f"Репетитор не может перенести занятие на это время.\n"
        f"Пожалуйста, выберите другое время.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"❌ Перенос отклонен!\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )

    await callback.answer("❌ Перенос отклонен")

async def cancel_lesson_handler(callback: types.CallbackQuery, state: FSMContext):
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
    await callback.message.edit_text("❌ Выберите занятие для отмены:", reply_markup=lessons_list_keyboard(lessons, "cancel_pick"))
    await callback.answer()

async def cancel_pick_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    lesson_id = callback.data.replace("cancel_pick_", "")
    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        return

    lesson = confirmed[lesson_id]
    student_id = callback.from_user.id

    # Восстанавливаем данные ученика
    student_info = get_student_info_from_any_source(student_id)

    if not student_info:
        student_name = lesson.get("student_name", "Ученик")
        student_class = lesson.get("student_class", "")
        print(f"⚠️ ВНИМАНИЕ: данные {student_id} восстановлены из lessons: {student_name} ({student_class})")
        cache_student_info(student_id, student_name, student_class)
    else:
        student_name = student_info["name"]
        student_class = student_info["grade"]

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

    print(f"📝 Создан запрос на отмену занятия: {cancel_id} - {student_name}")

    lesson_date_str = lesson["date_str"]
    lesson_time_str = lesson["time"]

    await bot.send_message(
        TUTOR_ID,
        f"❌ Запрос на отмену занятия!\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📚 Класс: {student_class}\n"
        f"📖 Предмет: {lesson['subject']}\n"
        f"📅 Дата: {lesson_date_str}\n"
        f"⏰ Время: {lesson_time_str}",
        reply_markup=tutor_cancel_confirm_keyboard(cancel_id),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Запрос на отмену отправлен!\n\n"
        f"Репетитор рассмотрит ваш запрос.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )

    await state.clear()
    await callback.answer()

async def confirm_cancel_handler(callback: types.CallbackQuery, bot: Bot):
    cancel_id = callback.data.replace("confirm_cancel_", "")
    pending_cancels = load_json(PENDING_CANCELS_FILE)

    if cancel_id not in pending_cancels:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return

    cancel = pending_cancels[cancel_id]

    lesson_id = cancel["lesson_id"]
    student_id = cancel["student_id"]
    student_name = cancel["student_name"]

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id in confirmed:
        del confirmed[lesson_id]
        save_json(CONFIRMED_FILE, confirmed)

    del pending_cancels[cancel_id]
    save_json(PENDING_CANCELS_FILE, pending_cancels)

    print(f"✅ Отмена занятия подтверждена: {cancel_id} - {student_name}")

    await bot.send_message(
        student_id,
        f"✅ Ваша заявка на отмену одобрена!\n\n"
        f"Занятие было отменено.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"✅ Отмена подтверждена!\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )

    await callback.answer("✅ Отмена подтверждена")

async def reject_cancel_handler(callback: types.CallbackQuery, bot: Bot):
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

    print(f"❌ Отмена занятия отклонена: {cancel_id} - {student_name}")

    await bot.send_message(
        student_id,
        f"❌ Запрос на отмену отклонен\n\n"
        f"Репетитор не отменяет это занятие.\n"
        f"Занятие остается в расписании.",
        reply_markup=persistent_menu_keyboard(),
        parse_mode="HTML"
    )

    await callback.message.edit_text(
        f"❌ Отмена отклонена!\n\n"
        f"Ученик {student_name} был уведомлен.",
        parse_mode="HTML"
    )

    await callback.answer("❌ Отмена отклонена")

async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    await callback.message.edit_text("📌 Главное меню", reply_markup=main_menu_keyboard(user_id))
    await callback.answer()

async def edit_schedule_button_handler(callback: types.CallbackQuery, state: FSMContext):
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
        "🛠 Интерактивное редактирование расписания\n\n"
        "Выберите день недели для редактирования:",
        reply_markup=kb,
        parse_mode="HTML"
    )

    await state.set_state(InteractiveScheduleStates.choosing_day)
    await callback.answer()

async def interactive_day_select_handler(callback: types.CallbackQuery, state: FSMContext):
    day_name = callback.data.replace("iday_", "")
    day_ru = DAYS_RU.get(day_name, day_name)

    await state.update_data(current_day=day_name)

    await callback.message.edit_text(
        f"📅 {day_ru}\n\n"
        f"Когда вы можете начать занятия в {day_ru}?\n\n"
        "Примеры:\n"
        "• 19:30 — начало в 19:30\n"
        "• 18 — начало в 18:00\n"
        "• нет — нет занятий в этот день\n\n"
        f"Бот автоматически создаст слоты по 1 часу (до {MAX_WORK_HOUR}:00)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_schedule_menu")]
        ]),
        parse_mode="HTML"
    )

    await state.set_state(InteractiveScheduleStates.waiting_for_start_time)
    await callback.answer()

async def interactive_time_input_handler(message: types.Message, state: FSMContext):
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
        message_text = f"✅ {day_ru}: нет занятий"
    else:
        start_h, start_m = time_input
        slots = generate_time_slots(start_h, start_m)
        interactive_schedule[day_name] = slots
        slots_str = ", ".join(slots)
        message_text = f"✅ {day_ru}:\n{slots_str}\n\n(автоматически созданы слоты по 1 часу)"

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
    data = await state.get_data()
    interactive_schedule = data.get("interactive_schedule", {})

    save_json(SCHEDULE_FILE, interactive_schedule)

    print(f"✅ Расписание сохранено: {interactive_schedule}")

    preview = format_schedule_for_preview(interactive_schedule)

    await callback.message.edit_text(
        "✅ Расписание успешно обновлено!\n\n" + preview,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )

    await state.clear()
    await callback.answer()

async def back_to_schedule_menu_handler(callback: types.CallbackQuery, state: FSMContext):
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
        "🛠 Интерактивное редактирование расписания\n\n"
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
    """✅ ИСПРАВЛЕНО: Отправлять напоминания о занятиях за ЧАС (60 минут)"""
    await asyncio.sleep(60)

    while True:
        try:
            now = datetime.now()
            confirmed = load_json(CONFIRMED_FILE)
            reminders_sent = load_json(REMINDERS_SENT_FILE)

            for lesson_id, lesson in confirmed.items():
                try:
                    lesson_time = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
                    time_diff = (lesson_time - now).total_seconds()

                    # ✅ ИСПРАВЛЕНО: Проверяем, находится ли занятие в диапазоне 50-70 минут до начала (за час ±10 минут)
                    if 3000 <= time_diff <= 4200:  # 50-70 минут
                        # Проверяем, не отправили ли мы уже напоминание для этого занятия
                        if lesson_id not in reminders_sent:
                            student_id = lesson.get("student_id")
                            student_name = lesson.get("student_name", "Гость")
                            subject = lesson.get("subject", "")
                            lesson_time_str = lesson_time.strftime("%H:%M")
                            lesson_date_str = lesson_time.strftime("%d.%m.%Y")

                            # ✅ ИСПРАВЛЕНО: Разные сообщения для ученика и репетитора
                            
                            # Сообщение для УЧЕНИКА (только предмет и время)
                            await bot.send_message(
                                student_id,
                                f"⏰ Напоминание!\n\n"
                                f"Через час начнется занятие по {subject}.\n"
                                f"Дата: {lesson_date_str}\n"
                                f"Время: {lesson_time_str}",
                                parse_mode="HTML",
                                reply_markup=persistent_menu_keyboard()
                            )

                            # Сообщение для РЕПЕТИТОРА (имя ученика, предмет и время)
                            await bot.send_message(
                                TUTOR_ID,
                                f"⏰ Напоминание!\n\n"
                                f"Через час занятие с {student_name} по {subject}.\n"
                                f"Дата: {lesson_date_str}\n"
                                f"Время: {lesson_time_str}",
                                parse_mode="HTML"
                            )

                            # Отмечаем, что напоминание отправлено
                            reminders_sent[lesson_id] = {
                                "timestamp": now.isoformat(),
                                "student_id": student_id,
                                "lesson_time": lesson_time.isoformat()
                            }
                            save_json(REMINDERS_SENT_FILE, reminders_sent)

                            print(f"📬 Напоминания отправлены: {student_name} - {subject} в {lesson_time_str}")

                except Exception as e:
                    print(f"⚠️ Ошибка при отправке напоминания для {lesson_id}: {e}")
                    pass

            await asyncio.sleep(300)

        except Exception as e:
            print(f"⚠️ Ошибка в send_reminders: {e}")
            await asyncio.sleep(60)

async def send_daily_schedule(bot: Bot):
    """Отправлять ежедневное расписание репетитору в 8:00"""
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
    """Очищать старые запросы каждый час"""
    await asyncio.sleep(300)

    while True:
        try:
            cleanup_stale_requests()
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"⚠️ Ошибка в cleanup_task: {e}")
            await asyncio.sleep(60)

async def cleanup_old_reminders(bot: Bot):
    """✅ НОВОЕ: Очищать старые записи о напоминаниях (старше 3 дней)"""
    await asyncio.sleep(600)

    while True:
        try:
            now = datetime.now()
            reminders_sent = load_json(REMINDERS_SENT_FILE)
            old_reminder_ids = []

            for reminder_id, reminder_data in reminders_sent.items():
                try:
                    reminder_time = datetime.fromisoformat(reminder_data.get("timestamp", ""))
                    if (now - reminder_time).total_seconds() > 259200:  # 3 дня
                        old_reminder_ids.append(reminder_id)
                except:
                    pass

            for reminder_id in old_reminder_ids:
                del reminders_sent[reminder_id]
                print(f"🗑️ Удалено старое напоминание: {reminder_id}")

            if old_reminder_ids:
                save_json(REMINDERS_SENT_FILE, reminders_sent)

            await asyncio.sleep(3600)

        except Exception as e:
            print(f"⚠️ Ошибка в cleanup_old_reminders: {e}")
            await asyncio.sleep(60)

async def keep_alive_task():
    """Отправлять ping для keep-alive каждые 14 минут"""
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
    return web.json_response({
        "status": "ok",
        "service": "tutor_bot",
        "timestamp": datetime.now().isoformat()
    })

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

            # Message handlers
            dp.message.register(start_handler, Command("start"))
            dp.message.register(menu_button_handler, F.text == "☰ Меню")
            dp.message.register(first_lesson_name_handler, FirstLessonStates.waiting_for_name)
            dp.message.register(first_lesson_class_handler, FirstLessonStates.waiting_for_class)
            dp.message.register(interactive_time_input_handler, InteractiveScheduleStates.waiting_for_start_time)

            # Callback handlers - основные действия
            dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")
            dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")
            dp.callback_query.register(reschedule_lesson_handler, F.data == "reschedule_lesson")
            dp.callback_query.register(cancel_lesson_handler, F.data == "cancel_lesson")
            dp.callback_query.register(my_schedule_handler, F.data == "my_schedule")
            dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")

            # Выбор предмета
            dp.callback_query.register(subject_single_handler, F.data.startswith("subject_single_"))

            # Выбор времени - первое занятие
            dp.callback_query.register(time_select_handler, F.data.startswith("time_"), FirstLessonStates.waiting_for_time)
            dp.callback_query.register(confirm_time_handler, F.data.startswith("confirm_time_"))

            # Повторное занятие
            dp.callback_query.register(repeat_time_select_handler, F.data.startswith("repeat_time_"), RepeatLessonStates.waiting_for_time)
            dp.callback_query.register(repeat_confirm_handler, F.data.startswith("repeat_confirm_"))

            # Перенос занятия
            dp.callback_query.register(reschedule_pick_handler, F.data.startswith("reschedule_pick_"), RescheduleStates.choosing_lesson)
            dp.callback_query.register(reschedule_day_handler, F.data.startswith("reschedule_day_"))
            dp.callback_query.register(reschedule_confirm_handler, F.data.startswith("reschedule_confirm_"))

            # Отмена занятия
            dp.callback_query.register(cancel_pick_handler, F.data.startswith("cancel_pick_"), CancelLessonStates.choosing_lesson)

            # Расписание репетитора
            dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
            dp.callback_query.register(interactive_day_select_handler, F.data.startswith("iday_"))
            dp.callback_query.register(interactive_save_handler, F.data == "isave_schedule")
            dp.callback_query.register(back_to_schedule_menu_handler, F.data == "back_to_schedule_menu")

            # Подтверждение/отклонение запросов репетитором
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
            asyncio.create_task(cleanup_old_reminders(bot))  # ✅ НОВОЕ
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

async def main():
    print("=" * 70)
    print("INITIALIZING APPLICATION - COMPLETE SYSTEM (FULLY FIXED v6)")
    print("✅ ИСПРАВЛЕНИЯ: Напоминания за 1 час, разные сообщения для ученика и репетитора")
    print("=" * 70)
    print(f"Port: {PORT}")
    print(f"Token: {'OK' if TOKEN else 'NOT SET'}")
    print(f"Render URL: {RENDER_URL if RENDER_URL else 'NOT SET'}")
    print(f"Max work hour: {MAX_WORK_HOUR}:00")
    print(f"Slot duration: {SLOT_DURATION} минут")
    print(f"Reminder time: {REMINDER_TIME_MINUTES} минут до занятия")
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

