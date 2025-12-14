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
RENDER_URL = os.getenv('RENDER_URL', '')

if not TOKEN:
    TOKEN = '7954650918:AAFZlRTRxZEUXNq_IYACCn60WIq8y2NBSdI'

TUTOR_ID = 1339816111
SUBJECTS = ["Математика", "Физика", "Химия"]
DEFAULT_SCHEDULE = {
    "Monday": [f"{h}:30" for h in range(19, 21)],
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

def get_student_info(student_id: int) -> Optional[Dict]:
    students = load_json(STUDENTS_FILE)
    return students.get(str(student_id))

def save_student_info(student_id: int, name: str, grade: str):
    students = load_json(STUDENTS_FILE)
    students[str(student_id)] = {"name": name, "grade": grade}
    save_json(STUDENTS_FILE, students)

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
        # Для репетитора показываем его расписание
        lessons = get_tutor_lessons()
        message_text = format_tutor_schedule_message(lessons)
    else:
        # Для ученика показываем его расписание
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
    """Обработчик кнопки 'Изменить расписание'"""
    await callback.message.edit_text(
        "📝 Отправьте JSON с расписанием в формате:\n"
        '{"Monday": ["18:00", "19:00"], ...}',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")]
        ])
    )
    
    await state.set_state(TutorScheduleStates.waiting_for_schedule_json)
    await callback.answer()

async def schedule_json_handler(message: types.Message, state: FSMContext):
    """Обработчик JSON расписания"""
    try:
        schedule = json.loads(message.text)
        
        if not isinstance(schedule, dict):
            await message.answer("❌ Расписание должно быть объектом JSON")
            return
        
        save_json(SCHEDULE_FILE, schedule)
        
        await message.answer(
            "✅ Расписание успешно обновлено!",
            reply_markup=main_menu_keyboard(TUTOR_ID)
        )
        
        await state.clear()
    except json.JSONDecodeError:
        await message.answer("❌ Неверный формат JSON. Попробуйте еще раз.")

# ============================================================================
# ЗАДАЧИ (SEND_REMINDERS, ETC)
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
        print("⚠️ RENDER_URL не установлен. Keep-alive отключен.")
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
                        else:
                            print(f"⚠️ Keep-alive ответ: {resp.status}")
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
            dp.message.register(schedule_json_handler, TutorScheduleStates.waiting_for_schedule_json)
            
            dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")
            dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")
            dp.callback_query.register(reschedule_lesson_handler, F.data == "reschedule_lesson")
            dp.callback_query.register(cancel_lesson_handler, F.data == "cancel_lesson")
            dp.callback_query.register(my_schedule_handler, F.data == "my_schedule")
            dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")
            dp.callback_query.register(subject_single_handler, F.data.startswith("subject_single_"))
            dp.callback_query.register(time_select_handler, F.data.startswith("time_"))
            dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
            
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
