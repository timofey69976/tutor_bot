# -*- coding: utf-8 -*-

import os
import asyncio
import sys
import json
import uuid
from datetime import datetime, timedelta, timezone
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
    TOKEN = '8388119061:AAEfeIhBSsD_3WyVS3L_YRtdbvbQxyf5RCM'

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

DAYS_RU = {
    "Monday": "Понедельник",
    "Tuesday": "Вторник",
    "Wednesday": "Среда",
    "Thursday": "Четверг",
    "Friday": "Пятница",
    "Saturday": "Суббота"
}

# 🔧 ИСПРАВЛЕНИЕ: Добавляем часовой пояс MSK (UTC+3)
MSK_TIMEZONE = timezone(timedelta(hours=3))

# ✅ ИСПРАВЛЕНО: Используем абсолютный путь вместо относительного
if os.path.exists('/app'):
    DATA_DIR = Path('/app/bot_data')
else:
    DATA_DIR = Path.cwd() / 'bot_data'

print(f"📂 DATA_DIR = {DATA_DIR}")
print(f"📂 Current working directory = {Path.cwd()}")

DATA_DIR.mkdir(parents=True, exist_ok=True)

STUDENTS_FILE = DATA_DIR / "students.json"
SCHEDULE_FILE = DATA_DIR / "schedule.json"
PENDING_FILE = DATA_DIR / "pending_requests.json"
CONFIRMED_FILE = DATA_DIR / "confirmed_lessons.json"
PENDING_RESCHEDULES_FILE = DATA_DIR / "pending_reschedules.json"
PENDING_CANCELS_FILE = DATA_DIR / "pending_cancels.json"
PENDING_TUTOR_RESCHEDULES_FILE = DATA_DIR / "pending_tutor_reschedules.json"

print(f"📝 Files will be saved to:")
print(f" - {STUDENTS_FILE}")
print(f" - {SCHEDULE_FILE}")
print(f" - {CONFIRMED_FILE}")
print(f" - {PENDING_FILE}\n")

# ✅ ИСПРАВЛЕНО: Теперь STUDENT_CACHE загружается из файла при запуске
STUDENT_CACHE = {}

# ✅ ИСПРАВЛЕНИЕ: Глобальный набор для отслеживания отправленных напоминаний
SENT_REMINDERS = set()

# ============================================================================
# ФУНКЦИИ РАБОТЫ С JSON
# ============================================================================

def load_json(filepath):
    """Безопасная загрузка JSON файла"""
    try:
        if filepath.exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"✅ Загружено: {filepath.name} ({len(data)} записей)")
                return data
    except Exception as e:
        print(f"⚠️ Ошибка при загрузке {filepath}: {e}")
    
    return {}

def save_json(filepath, data):
    """Безопасное сохранение JSON файла с проверкой"""
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        if not data and filepath.name not in ["pending_requests.json", "pending_reschedules.json", "pending_cancels.json", "pending_tutor_reschedules.json"]:
            print(f"⚠️ ВНИМАНИЕ: Попытка сохранить пустые данные в {filepath.name}")
            if filepath.name in ["schedule.json", "confirmed_lessons.json"]:
                print(f" ⛔ ОТМЕНЕНО: Сохранение отменено для защиты данных")
                return
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        if filepath.exists():
            file_size = filepath.stat().st_size
            print(f"✅ Сохранено: {filepath.name} ({file_size} байт, {len(data)} записей)")
        else:
            print(f"❌ ОШИБКА: Файл не был создан: {filepath}")
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА при сохранении {filepath}: {e}")
        import traceback
        traceback.print_exc()

def cleanup_stale_requests():
    """Удаление старых запросов старше 24 часов"""
    now = datetime.now(tz=MSK_TIMEZONE)
    
    for filepath in [PENDING_FILE, PENDING_RESCHEDULES_FILE, PENDING_CANCELS_FILE, PENDING_TUTOR_RESCHEDULES_FILE]:
        data = load_json(filepath)
        stale_ids = []
        
        for req_id, req in data.items():
            try:
                req_time = datetime.fromisoformat(req.get("timestamp", ""))
                if req_time.tzinfo is None:
                    req_time = req_time.replace(tzinfo=MSK_TIMEZONE)
                
                if (now - req_time).total_seconds() > 86400:
                    stale_ids.append(req_id)
            except:
                pass
        
        for req_id in stale_ids:
            del data[req_id]
            print(f"🗑️ Удален старый запрос: {req_id}")
        
        if stale_ids:
            save_json(filepath, data)

def cleanup_sent_reminders_list():
    """Очистить отправленные напоминания старше 2 часов"""
    global SENT_REMINDERS
    
    now = datetime.now(tz=MSK_TIMEZONE)
    active_reminders = set()
    
    for reminder_key in SENT_REMINDERS:
        try:
            parts = reminder_key.split(":", 1)
            if len(parts) == 2:
                lesson_id, timestamp_str = parts
                timestamp = datetime.fromisoformat(timestamp_str)
                
                if (now - timestamp).total_seconds() < 7200:
                    active_reminders.add(reminder_key)
        except Exception as e:
            print(f"⚠️ Ошибка при очистке напоминания {reminder_key}: {e}")
    
    SENT_REMINDERS = active_reminders
    print(f"🧹 Очищены старые напоминания. Активных: {len(SENT_REMINDERS)}")

# ============================================================================
# ✅ НОВОЕ: ВОССТАНОВЛЕНИЕ КЕША ИЗ ФАЙЛОВ ПРИ ЗАПУСКЕ
# ============================================================================

def restore_cache_from_files():
    """✅ НОВАЯ ФУНКЦИЯ: Восстановить STUDENT_CACHE из всех файлов при запуске"""
    global STUDENT_CACHE
    
    print("🔄 Восстанавливаю STUDENT_CACHE из файлов...")
    
    # 1. Из students.json
    students_data = load_json(STUDENTS_FILE)
    for student_id_str, student_info in students_data.items():
        try:
            student_id = int(student_id_str)
            if student_id != TUTOR_ID:
                STUDENT_CACHE[student_id] = {
                    "name": student_info.get("name", ""),
                    "grade": student_info.get("grade", "")
                }
        except:
            pass
    
    # 2. Из confirmed_lessons.json
    confirmed_data = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed_data.items():
        student_id = lesson.get("student_id")
        if student_id and student_id != TUTOR_ID and student_id not in STUDENT_CACHE:
            STUDENT_CACHE[student_id] = {
                "name": lesson.get("student_name", ""),
                "grade": lesson.get("student_class", "")
            }
    
    # 3. Из pending_requests.json
    pending_data = load_json(PENDING_FILE)
    for req_id, req in pending_data.items():
        student_id = req.get("student_id")
        if student_id and student_id != TUTOR_ID and student_id not in STUDENT_CACHE:
            STUDENT_CACHE[student_id] = {
                "name": req.get("student_name", ""),
                "grade": req.get("student_class", "")
            }
    
    print(f"✅ STUDENT_CACHE восстановлен: {len(STUDENT_CACHE)} записей")

# ============================================================================
# ФУНКЦИИ ОТПРАВКИ СООБЩЕНИЙ
# ============================================================================

async def send_reminders(bot: Bot):
    """Отправлять напоминание за 60 минут до занятия (ТОЛЬКО ОДИН РАЗ)"""
    await asyncio.sleep(15)
    
    while True:
        try:
            now = datetime.now(tz=MSK_TIMEZONE)
            confirmed = load_json(CONFIRMED_FILE)
            
            for lesson_id, lesson in confirmed.items():
                try:
                    lesson_time = datetime.fromisoformat(lesson.get('lesson_datetime', ''))
                    
                    if lesson_time.tzinfo is None:
                        lesson_time = lesson_time.replace(tzinfo=MSK_TIMEZONE)
                    
                    time_diff = (lesson_time - now).total_seconds()
                    
                    # За 60±5 минут до занятия (3480-3720 секунд это 58-62 минуты)
                    if 3480 <= time_diff <= 3720:
                        reminder_key = f"{lesson_id}:{lesson_time.isoformat()}"
                        
                        if reminder_key not in SENT_REMINDERS:
                            student_id = lesson.get('student_id')
                            student_name = lesson.get('student_name')
                            subject = lesson.get('subject')
                            lesson_time_str = lesson_time.strftime('%H:%M')
                            
                            print(f"📤 Отправляю напоминание для занятия {lesson_id}")
                            
                            # Напоминание для ученика:
                            await bot.send_message(
                                student_id,
                                f"⏰ Напоминание о занятии!\n\n"
                                f"Предмет: {subject}\n"
                                f"Время: {lesson_time_str}\n\n"
                                f"Занятие начинается через 1 час! 📚",
                                parse_mode="HTML",
                                reply_markup=persistent_menu_keyboard()
                            )
                            
                            # Напоминание для репетитора:
                            await bot.send_message(
                                TUTOR_ID,
                                f"⏰ Напоминание о занятии!\n\n"
                                f"Ученик: {student_name}\n"
                                f"Предмет: {subject}\n"
                                f"Время: {lesson_time_str}\n\n"
                                f"Занятие начинается через 1 час! 📚",
                                parse_mode="HTML",
                                reply_markup=persistent_menu_keyboard()
                            )
                            
                            SENT_REMINDERS.add(reminder_key)
                            print(f"✅ Напоминание отправлено и запомнено")
                        else:
                            print(f"⏭️ Напоминание для {lesson_id} уже отправлено, пропускаем")
                
                except Exception as e:
                    print(f"⚠️ Ошибка при обработке напоминания {lesson_id}: {e}")
            
            await asyncio.sleep(60)
            
            # Каждые 10 минут очищаем старые напоминания
            if int(now.timestamp()) % 600 == 0:
                cleanup_sent_reminders_list()
        
        except Exception as e:
            print(f"⚠️ Ошибка в send_reminders: {e}")
            await asyncio.sleep(60)

async def send_daily_schedule(bot: Bot):
    """✅ ИСПРАВЛЕНО: Отправлять ежедневное расписание репетитору в 8:00 с занятиями только на сегодня"""
    await asyncio.sleep(120)
    
    while True:
        try:
            now = datetime.now(tz=MSK_TIMEZONE)
            
            # ✅ ИСПРАВЛЕНО: Проверяем время в 8:00
            if now.hour == 8 and 0 <= now.minute < 5:
                print(f"📅 Отправляю расписание на сегодня в {now.strftime('%H:%M:%S')}")
                
                # Получаем все подтвержденные занятия
                all_lessons = load_json(CONFIRMED_FILE)
                
                # ✅ ИСПРАВЛЕНО: Фильтруем только сегодняшние занятия
                today_date = now.date()
                today_lessons = []
                
                for lesson_id, lesson in all_lessons.items():
                    try:
                        lesson_datetime = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
                        
                        if lesson_datetime.tzinfo is None:
                            lesson_datetime = lesson_datetime.replace(tzinfo=MSK_TIMEZONE)
                        
                        # Проверяем, что занятие сегодня
                        if lesson_datetime.date() == today_date:
                            today_lessons.append((lesson_datetime, lesson))
                    except Exception as e:
                        print(f"⚠️ Ошибка при обработке занятия {lesson_id}: {e}")
                
                # Сортируем занятия по времени
                today_lessons.sort(key=lambda x: x[0])
                
                # Получаем название дня недели на русском
                weekday_names = {
                    0: "Понедельник",
                    1: "Вторник",
                    2: "Среда",
                    3: "Четверг",
                    4: "Пятница",
                    5: "Суббота",
                    6: "Воскресенье"
                }
                
                day_name = weekday_names.get(now.weekday(), "")
                
                if today_lessons:
                    message = f"📚 <b>Расписание на сегодня</b>\n\n{day_name}, {now.strftime('%d.%m.%Y')}\n\n"
                    
                    for lesson_datetime, lesson in today_lessons:
                        time_str = lesson_datetime.strftime("%H:%M")
                        student_name = lesson.get("student_name", "Неизвестный ученик")
                        student_class = lesson.get("student_class", "")
                        subject = lesson.get("subject", "Неизвестный предмет")
                        
                        message += f"🕐 {time_str} - {student_name}, {student_class}, {subject}\n"
                else:
                    message = f"📭 На сегодня ({day_name}) нет занятий"
                
                await bot.send_message(
                    TUTOR_ID,
                    message,
                    parse_mode="HTML",
                    reply_markup=persistent_menu_keyboard()
                )
                
                print(f"✅ Расписание отправлено успешно")
                await asyncio.sleep(3600)
            else:
                await asyncio.sleep(60)
        
        except Exception as e:
            print(f"⚠️ Ошибка в send_daily_schedule: {e}")
            await asyncio.sleep(60)

async def cleanup_task(bot: Bot):
    """Очищать старые запросы каждый час"""
    await asyncio.sleep(300)
    
    while True:
        try:
            print(f"🧹 Запускаю очистку старых запросов [{datetime.now().strftime('%H:%M:%S')}]")
            cleanup_stale_requests()
            print(f"✅ Очистка завершена")
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"⚠️ Ошибка в cleanup_task: {e}")
            await asyncio.sleep(60)

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

class TutorRescheduleStates(StatesGroup):
    choosing_lesson = State()
    waiting_for_new_time = State()

class BroadcastMessageStates(StatesGroup):
    waiting_for_message = State()

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
        # ✅ ИСПРАВЛЕНО: Кнопка для отправки просьбы о переносе ученику
        kb.inline_keyboard.append(
            [InlineKeyboardButton(text="📬 Просьба о переносе", callback_data="tutor_reschedule_request")]
        )
        # ✅ ИСПРАВЛЕНО: Кнопка для отправки сообщения всем ученикам
        kb.inline_keyboard.append(
            [InlineKeyboardButton(text="📢 Уведомить всех", callback_data="broadcast_message")]
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
# ФУНКЦИИ УПРАВЛЕНИЯ ДАННЫМИ
# ============================================================================

def cache_student_info(student_id: int, name: str, grade: str):
    """Кешировать данные в памяти + сохранить в файл"""
    STUDENT_CACHE[student_id] = {"name": name, "grade": grade}
    
    students = load_json(STUDENTS_FILE)
    students[str(student_id)] = {"name": name, "grade": grade}
    save_json(STUDENTS_FILE, students)
    
    print(f"✅ Кешировано и сохранено: {name} ({grade}) - ID: {student_id}")

def get_student_info_from_any_source(student_id: int) -> Optional[Dict]:
    """Получить данные ученика из ЛЮБОГО источника по приоритету"""
    if student_id in STUDENT_CACHE:
        info = STUDENT_CACHE[student_id]
        print(f"✅ Найдено в памяти: {info['name']} ({info['grade']}) - ID: {student_id}")
        return info
    
    students = load_json(STUDENTS_FILE)
    if str(student_id) in students:
        info = students[str(student_id)]
        STUDENT_CACHE[student_id] = info
        print(f"✅ Найдено в students.json: {info['name']} ({info['grade']}) - ID: {student_id}")
        return info
    
    confirmed = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed.items():
        if lesson.get("student_id") == student_id:
            name = lesson.get("student_name", "")
            grade = lesson.get("student_class", "")
            if name and grade:
                info = {"name": name, "grade": grade}
                cache_student_info(student_id, name, grade)
                print(f"✅ Восстановлено из confirmed lessons: {name} ({grade}) - ID: {student_id}")
                return info
    
    pending = load_json(PENDING_FILE)
    for req_id, req in pending.items():
        if req.get("student_id") == student_id:
            name = req.get("student_name", "")
            grade = req.get("student_class", "")
            if name and grade:
                info = {"name": name, "grade": grade}
                cache_student_info(student_id, name, grade)
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
    """Получить даты текущей недели (понедельник - суббота) с правильным часовым поясом"""
    if start_date is None:
        start_date = datetime.now(tz=MSK_TIMEZONE)
    
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=MSK_TIMEZONE)
    
    current_weekday = start_date.weekday()
    
    if current_weekday == 6:
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
    """Получить все забронированные времена по датам и времени"""
    booked: Dict[str, bool] = {}
    
    confirmed = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed.items():
        try:
            lesson_datetime = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
            
            if lesson_datetime.tzinfo is None:
                lesson_datetime = lesson_datetime.replace(tzinfo=MSK_TIMEZONE)
            
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
    """✅ ИСПРАВЛЕНО: Получить доступные времена для дня"""
    all_times = schedule.get(day_name, [])
    
    # ✅ ИСПРАВЛЕНО: Если день содержит "нет", то нет доступных времен
    if isinstance(all_times, str) and all_times == "нет":
        return []
    
    if not all_times:
        return []
    
    available = [time for time in all_times if not is_time_slot_booked(day_name, time)]
    
    return available

def create_request_id():
    """Создать уникальный ID запроса"""
    return str(uuid.uuid4())[:8]

def parse_time(time_str: str) -> tuple:
    """Парсить время из строки HH:MM"""
    parts = time_str.split(":")
    return int(parts[0]), int(parts[1])

def get_lesson_datetime(day_name: str, time_str: str) -> Optional[datetime]:
    """Получить datetime для занятия с правильным часовым поясом MSK"""
    week = get_week_dates()
    
    if day_name not in week:
        return None
    
    date_obj, _ = week[day_name]
    
    hour, minute = parse_time(time_str)
    
    dt = date_obj.replace(hour=hour, minute=minute, second=0, microsecond=0, tzinfo=MSK_TIMEZONE)
    
    return dt

def get_student_lessons(student_id: int) -> Dict:
    """Получить подтвержденные занятия ученика"""
    confirmed = load_json(CONFIRMED_FILE)
    return {lid: l for lid, l in confirmed.items() if l.get("student_id") == student_id}

def get_tutor_lessons() -> Dict:
    """Получить занятия репетитора на эту неделю"""
    confirmed = load_json(CONFIRMED_FILE)
    week = get_week_dates()
    
    tutor_lessons = {}
    
    for lesson_id, lesson in confirmed.items():
        try:
            lesson_date = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
            
            if lesson_date.tzinfo is None:
                lesson_date = lesson_date.replace(tzinfo=MSK_TIMEZONE)
            
            week_start = week["Monday"][0]
            week_end = week["Saturday"][0] + timedelta(days=1)
            
            if week_start <= lesson_date < week_end:
                tutor_lessons[lesson_id] = lesson
        
        except Exception as e:
            print(f"⚠️ Ошибка при обработке tutor lesson {lesson_id}: {e}")
            pass
    
    return tutor_lessons

def get_all_students() -> Dict[int, Dict]:
    """✅ ИСПРАВЛЕНО: Получить всех, кто хотя бы раз пользовался ботом"""
    all_students = {}
    
    # 1. Сначала из students.json (те, кто заполнял данные)
    students_file_data = load_json(STUDENTS_FILE)
    for student_id_str, student_data in students_file_data.items():
        try:
            student_id = int(student_id_str)
            if student_id not in all_students and student_id != TUTOR_ID:
                all_students[student_id] = {
                    "name": student_data.get("name", "Ученик"),
                    "class": student_data.get("grade", "")
                }
        except:
            pass
    
    # 2. Затем из confirmed lessons (те, у кого были занятия)
    confirmed = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed.items():
        student_id = lesson.get("student_id")
        if student_id and student_id not in all_students and student_id != TUTOR_ID:
            all_students[student_id] = {
                "name": lesson.get("student_name", "Ученик"),
                "class": lesson.get("student_class", "")
            }
    
    # 3. Затем из pending requests (те, кто отправлял запросы)
    pending = load_json(PENDING_FILE)
    for req_id, req in pending.items():
        student_id = req.get("student_id")
        if student_id and student_id not in all_students and student_id != TUTOR_ID:
            all_students[student_id] = {
                "name": req.get("student_name", "Ученик"),
                "class": req.get("student_class", "")
            }
    
    # 4. Также из STUDENT_CACHE (те, кто есть в памяти)
    for student_id in STUDENT_CACHE:
        if student_id not in all_students and student_id != TUTOR_ID:
            all_students[student_id] = STUDENT_CACHE[student_id]
    
    print(f"📊 Всего найдено учеников: {len(all_students)}")
    return all_students

def format_student_schedule_message(lessons: Dict) -> str:
    """Форматировать расписание ученика"""
    if not lessons:
        return "📭 У вас нет занятий на эту неделю."
    
    message = "📚 Ваше расписание на эту неделю:\n\n"
    
    sorted_lessons = sorted(lessons.values(), key=lambda x: x.get("lesson_datetime", ""))
    
    for lesson in sorted_lessons:
        try:
            lesson_date = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
            
            if lesson_date.tzinfo is None:
                lesson_date = lesson_date.replace(tzinfo=MSK_TIMEZONE)
            
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
            lesson_date = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
            
            if lesson_date.tzinfo is None:
                lesson_date = lesson_date.replace(tzinfo=MSK_TIMEZONE)
            
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

router = Dispatcher()

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
        print(f"⚠️ Расписание пусто! Используем DEFAULT_SCHEDULE")
    
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
        
        # ✅ ИСПРАВЛЕНО: Показываем только дни с доступными временами
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
    
    if is_time_slot_booked(day_name, time_str):
        await callback.answer("❌ Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return
    
    lesson_datetime = get_lesson_datetime(day_name, time_str)
    
    if not lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return
    
    cache_student_info(student_id, student_name, student_class)
    
    request_id = create_request_id()
    
    pending = load_json(PENDING_FILE)
    pending[request_id] = {
        "student_id": student_id,
        "student_name": student_name,
        "student_class": student_class,
        "subject": subject,
        "lesson_datetime": lesson_datetime.isoformat(),
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat(),
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
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat()
    }
    
    save_json(CONFIRMED_FILE, confirmed)
    
    del pending[request_id]
    save_json(PENDING_FILE, pending)
    
    print(f"✅ Занятие подтверждено: {lesson_id} - {student_name}")
    
    lesson_datetime = datetime.fromisoformat(lesson_datetime_str)
    if lesson_datetime.tzinfo is None:
        lesson_datetime = lesson_datetime.replace(tzinfo=MSK_TIMEZONE)
    
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
    
    student_info = get_student_info_from_any_source(student_id)
    
    if not student_info:
        await callback.answer("❌ Ошибка: данные ученика не найдены. Пожалуйста, сначала запишитесь на первое занятие!", show_alert=True)
        return
    
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
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat(),
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

# ============================================================================
# ✅ НОВОЕ: ПРОСЬБА О ПЕРЕНОСЕ ОТ РЕПЕТИТОРА
# ============================================================================

async def tutor_reschedule_request_handler(callback: types.CallbackQuery, state: FSMContext):
    """Репетитор выбирает занятие для переноса"""
    lessons = get_tutor_lessons()
    
    if not lessons:
        await callback.message.edit_text(
            "❌ У вас нет занятий на эту неделю для переноса.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()
        return
    
    await state.set_state(TutorRescheduleStates.choosing_lesson)
    
    await callback.message.edit_text(
        "📅 Выберите занятие, которое нужно перенести:",
        reply_markup=lessons_list_keyboard(lessons, "tutor_reschedule_pick")
    )
    await callback.answer()

async def tutor_reschedule_pick_handler(callback: types.CallbackQuery, state: FSMContext):
    """Репетитор выбрал занятие, теперь выбирает новый день"""
    lesson_id = callback.data.replace("tutor_reschedule_pick_", "")
    
    confirmed = load_json(CONFIRMED_FILE)
    
    if lesson_id not in confirmed:
        await callback.answer("❌ Занятие не найдено", show_alert=True)
        return
    
    lesson = confirmed[lesson_id]
    student_name = lesson.get("student_name", "")
    subject = lesson.get("subject", "")
    
    await state.update_data(
        tutor_reschedule_lesson_id=lesson_id,
        tutor_reschedule_student_id=lesson.get("student_id"),
        tutor_reschedule_student_name=student_name,
        tutor_reschedule_subject=subject
    )
    
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
                InlineKeyboardButton(text=btn_text, callback_data=f"tutor_reschedule_day_{day_name}")
            ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text(
        f"📅 Выберите новый день для {student_name} ({subject}):",
        reply_markup=kb
    )
    await state.set_state(TutorRescheduleStates.waiting_for_new_time)
    await callback.answer()

async def tutor_reschedule_day_handler(callback: types.CallbackQuery, state: FSMContext):
    """Репетитор выбрал день, теперь выбирает время"""
    day_name = callback.data.replace("tutor_reschedule_day_", "")
    
    schedule = load_json(SCHEDULE_FILE)
    if not schedule:
        schedule = DEFAULT_SCHEDULE
    
    times = get_available_times(day_name, schedule)
    
    if not times:
        await callback.answer("❌ На этот день нет доступных свободных времен")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=time, callback_data=f"tutor_reschedule_confirm_{day_name}_{time}")] for time in times
    ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])
    
    await callback.message.edit_text("⏰ Выберите новое время:", reply_markup=kb)
    await callback.answer()

async def tutor_reschedule_confirm_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Репетитор отправляет ученику просьбу о переносе"""
    parts = callback.data.split("_")
    day_name = parts[3]
    time_str = "_".join(parts[4:])
    
    data = await state.get_data()
    
    lesson_id = data.get("tutor_reschedule_lesson_id")
    student_id = data.get("tutor_reschedule_student_id")
    student_name = data.get("tutor_reschedule_student_name")
    subject = data.get("tutor_reschedule_subject")
    
    if is_time_slot_booked(day_name, time_str):
        await callback.answer("❌ Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return
    
    new_lesson_datetime = get_lesson_datetime(day_name, time_str)
    
    if not new_lesson_datetime:
        await callback.answer("❌ Ошибка: не удалось определить время занятия")
        return
    
    reschedule_id = create_request_id()
    
    pending_tutor_reschedules = load_json(PENDING_TUTOR_RESCHEDULES_FILE)
    pending_tutor_reschedules[reschedule_id] = {
        "lesson_id": lesson_id,
        "student_id": student_id,
        "student_name": student_name,
        "subject": subject,
        "new_lesson_datetime": new_lesson_datetime.isoformat(),
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat(),
        "status": "pending"
    }
    
    save_json(PENDING_TUTOR_RESCHEDULES_FILE, pending_tutor_reschedules)
    
    lesson_date_str = new_lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = new_lesson_datetime.strftime("%H:%M")
    
    # Отправляем просьбу ученику о переносе
    kb_student = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Согласен", callback_data=f"student_reschedule_agree_{reschedule_id}")],
        [InlineKeyboardButton(text="❌ Не согласен", callback_data=f"student_reschedule_decline_{reschedule_id}")]
    ])
    
    await bot.send_message(
        student_id,
        f"📬 <b>Просьба о переносе занятия</b>\n\n"
        f"Репетитор просит перенести занятие:\n\n"
        f"📖 Предмет: {subject}\n"
        f"📅 Новая дата: {lesson_date_str}\n"
        f"⏰ Новое время: {lesson_time_str}\n\n"
        f"Вы согласны на перенос?",
        reply_markup=kb_student,
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        f"✅ Просьба отправлена ученику {student_name}!\n\n"
        f"Ожидаем ответа на предложенное время:\n"
        f"{lesson_date_str} в {lesson_time_str}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ])
    )
    
    await state.clear()
    await callback.answer()

async def student_reschedule_agree_handler(callback: types.CallbackQuery, bot: Bot):
    """Ученик согласился на перенос"""
    reschedule_id = callback.data.replace("student_reschedule_agree_", "")
    
    pending_tutor_reschedules = load_json(PENDING_TUTOR_RESCHEDULES_FILE)
    
    if reschedule_id not in pending_tutor_reschedules:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    reschedule = pending_tutor_reschedules[reschedule_id]
    
    lesson_id = reschedule["lesson_id"]
    student_id = reschedule["student_id"]
    student_name = reschedule["student_name"]
    new_datetime_str = reschedule["new_lesson_datetime"]
    
    confirmed = load_json(CONFIRMED_FILE)
    
    if lesson_id in confirmed:
        confirmed[lesson_id]["lesson_datetime"] = new_datetime_str
        
        new_datetime = datetime.fromisoformat(new_datetime_str)
        if new_datetime.tzinfo is None:
            new_datetime = new_datetime.replace(tzinfo=MSK_TIMEZONE)
        
        confirmed[lesson_id]["date_str"] = new_datetime.strftime("%d.%m.%Y")
        confirmed[lesson_id]["time"] = new_datetime.strftime("%H:%M")
        
        save_json(CONFIRMED_FILE, confirmed)
    
    del pending_tutor_reschedules[reschedule_id]
    save_json(PENDING_TUTOR_RESCHEDULES_FILE, pending_tutor_reschedules)
    
    new_datetime = datetime.fromisoformat(new_datetime_str)
    if new_datetime.tzinfo is None:
        new_datetime = new_datetime.replace(tzinfo=MSK_TIMEZONE)
    
    date_str = new_datetime.strftime("%d.%m.%Y")
    time_str = new_datetime.strftime("%H:%M")
    
    await bot.send_message(
        TUTOR_ID,
        f"✅ Ученик {student_name} согласился на перенос!\n\n"
        f"📅 Дата: {date_str}\n"
        f"⏰ Время: {time_str}",
        reply_markup=persistent_menu_keyboard()
    )
    
    await callback.message.edit_text(
        f"✅ Вы согласились на перенос занятия!\n\n"
        f"📅 Новая дата: {date_str}\n"
        f"⏰ Новое время: {time_str}\n\n"
        f"Репетитор был уведомлен.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ])
    )
    
    await callback.answer("✅ Вы согласились на перенос")

async def student_reschedule_decline_handler(callback: types.CallbackQuery, bot: Bot):
    """Ученик не согласился на перенос"""
    reschedule_id = callback.data.replace("student_reschedule_decline_", "")
    
    pending_tutor_reschedules = load_json(PENDING_TUTOR_RESCHEDULES_FILE)
    
    if reschedule_id not in pending_tutor_reschedules:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    reschedule = pending_tutor_reschedules[reschedule_id]
    
    student_name = reschedule["student_name"]
    
    del pending_tutor_reschedules[reschedule_id]
    save_json(PENDING_TUTOR_RESCHEDULES_FILE, pending_tutor_reschedules)
    
    await bot.send_message(
        TUTOR_ID,
        f"❌ Ученик {student_name} не согласился на перенос занятия.",
        reply_markup=persistent_menu_keyboard()
    )
    
    await callback.message.edit_text(
        f"❌ Вы отклонили просьбу о переносе.\n\n"
        f"Репетитор был уведомлен.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 В главное меню", callback_data="back_to_menu")]
        ])
    )
    
    await callback.answer("❌ Вы отклонили просьбу")

# ============================================================================
# ✅ НОВОЕ: УВЕДОМЛЕНИЕ ВСЕХ УЧЕНИКОВ
# ============================================================================

async def broadcast_message_handler(callback: types.CallbackQuery, state: FSMContext):
    """Репетитор начинает отправку сообщения всем"""
    await state.set_state(BroadcastMessageStates.waiting_for_message)
    
    await callback.message.edit_text(
        "📝 Напишите сообщение для всех учеников:\n\n"
        "(Нажмите /cancel для отмены)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_menu")]
        ])
    )
    
    await callback.answer()

async def broadcast_text_handler(message: types.Message, state: FSMContext, bot: Bot):
    """Репетитор ввел текст сообщения"""
    current_state = await state.get_state()
    
    if current_state != BroadcastMessageStates.waiting_for_message:
        return
    
    text = message.text.strip()
    
    if not text:
        await message.answer("❌ Пожалуйста, введите текст сообщения")
        return
    
    # Получаем всех студентов
    students = get_all_students()
    
    if not students:
        await message.answer(
            "❌ У вас нет студентов для уведомления.",
            reply_markup=persistent_menu_keyboard()
        )
        await state.clear()
        return
    
    sent_count = 0
    failed_count = 0
    
    for student_id, student_info in students.items():
        try:
            await bot.send_message(
                student_id,
                f"📢 <b>Сообщение от репетитора</b>\n\n{text}",
                parse_mode="HTML",
                reply_markup=persistent_menu_keyboard()
            )
            sent_count += 1
        except Exception as e:
            print(f"⚠️ Ошибка при отправке ученику {student_id}: {e}")
            failed_count += 1
    
    await message.answer(
        f"✅ Сообщение отправлено!\n\n"
        f"✉️ Успешно отправлено: {sent_count}\n"
        f"❌ Ошибок: {failed_count}",
        reply_markup=persistent_menu_keyboard()
    )
    
    await state.clear()

# ============================================================================
# ОСТАЛЬНЫЕ ОБРАБОТЧИКИ (ПЕРЕНОС И ОТМЕНА)
# ============================================================================

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
    
    if is_time_slot_booked(day_name, time_str):
        await callback.answer("❌ Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return
    
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
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat(),
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
    
    cache_student_info(student_id, student_name, student_class)
    
    confirmed = load_json(CONFIRMED_FILE)
    
    if lesson_id in confirmed:
        confirmed[lesson_id]["lesson_datetime"] = new_datetime_str
        
        new_datetime = datetime.fromisoformat(new_datetime_str)
        if new_datetime.tzinfo is None:
            new_datetime = new_datetime.replace(tzinfo=MSK_TIMEZONE)
        
        confirmed[lesson_id]["date_str"] = new_datetime.strftime("%d.%m.%Y")
        confirmed[lesson_id]["time"] = new_datetime.strftime("%H:%M")
        
        save_json(CONFIRMED_FILE, confirmed)
    
    del pending_reschedules[reschedule_id]
    save_json(PENDING_RESCHEDULES_FILE, pending_reschedules)
    
    print(f"✅ Перенос занятия подтвержден: {reschedule_id} - {student_name} ({student_class})")
    
    new_datetime = datetime.fromisoformat(new_datetime_str)
    if new_datetime.tzinfo is None:
        new_datetime = new_datetime.replace(tzinfo=MSK_TIMEZONE)
    
    date_str = new_datetime.strftime("%d.%m.%Y")
    time_str = new_datetime.strftime("%H:%M")
    
    await bot.send_message(
        student_id,
        f"✅ Ваш запрос на перенос подтвержден!\n\n"
        f"📅 Новая дата: {date_str}\n"
        f"⏰ Новое время: {time_str}\n"
        f"📖 Предмет: {subject}",
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
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat(),
        "status": "pending"
    }
    
    save_json(PENDING_CANCELS_FILE, pending_cancels)
    
    print(f"📝 Создан запрос на отмену занятия: {cancel_id} - {student_name}")
    
    lesson_datetime = datetime.fromisoformat(lesson["lesson_datetime"])
    if lesson_datetime.tzinfo is None:
        lesson_datetime = lesson_datetime.replace(tzinfo=MSK_TIMEZONE)
    
    lesson_date_str = lesson_datetime.strftime("%d.%m.%Y")
    lesson_time_str = lesson_datetime.strftime("%H:%M")
    
    await bot.send_message(
        TUTOR_ID,
        f"📋 Запрос на отмену занятия!\n\n"
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
        f"✅ Ваша отмена подтверждена!\n\n"
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
        f"Занятие остается в расписании.\n"
        f"Если у вас возникли проблемы, свяжитесь с репетитором.",
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

async def interactive_day_select_handler(callback: types.CallbackQuery, state: FSMContext):
    day_name = callback.data.replace("iday_", "")
    day_ru = DAYS_RU.get(day_name, day_name)
    
    await state.update_data(current_day=day_name)
    
    await callback.message.edit_text(
        f"📅 {day_ru}\n"
        f"Введите начальное время для {day_ru}?\n"
        f"(Например: 19:30 или 19:30 или 18 или 18:00)\n"
        f"Или напишите 'нет' для отмены",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню расписания", callback_data="back_to_schedule_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.waiting_for_start_time)
    
    await callback.answer()

async def interactive_save_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    interactive_schedule = data.get("interactive_schedule", {})
    
    print(f"📊 Сохраняю расписание: {interactive_schedule}")
    
    save_json(SCHEDULE_FILE, interactive_schedule)
    
    verification = load_json(SCHEDULE_FILE)
    print(f"📊 Проверка: {verification}")
    
    await callback.message.edit_text(
        f"✅ Расписание успешно сохранено!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="back_to_menu")]
        ]),
        parse_mode="HTML"
    )
    
    await state.clear()
    
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
        await message.answer("❌ Неправильный формат времени. Попробуйте еще. Примеры: 19:30, 19 или 18:00")
        return
    
    if time_input is None:
        interactive_schedule[day_name] = "нет"
        message_text = f"❌ На {day_ru} теперь нет занятий"
    else:
        start_h, start_m = time_input
        slots = generate_time_slots(start_h, start_m)
        
        interactive_schedule[day_name] = slots
        
        slots_str = ", ".join(slots)
        message_text = f"✅ Для {day_ru} установлены слоты:\n{slots_str}"
    
    print(f"📊 {interactive_schedule}")
    
    await state.update_data(interactive_schedule=interactive_schedule)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Понедельник", callback_data="iday_Monday"),
         InlineKeyboardButton(text="📅 Вторник", callback_data="iday_Tuesday")],
        [InlineKeyboardButton(text="📅 Среда", callback_data="iday_Wednesday"),
         InlineKeyboardButton(text="📅 Четверг", callback_data="iday_Thursday")],
        [InlineKeyboardButton(text="📅 Пятница", callback_data="iday_Friday"),
         InlineKeyboardButton(text="📅 Суббота", callback_data="iday_Saturday")],
        [InlineKeyboardButton(text="✅ Сохранить расписание", callback_data="save_schedule")],
        [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="back_to_menu")]
    ])
    
    await message.answer(message_text, reply_markup=kb, parse_mode="HTML")
    
    await state.set_state(InteractiveScheduleStates.choosing_day)

async def edit_schedule_button_handler(callback: types.CallbackQuery, state: FSMContext):
    current_schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE
    
    print(f"📊 {current_schedule}")
    
    await state.update_data(interactive_schedule=current_schedule.copy(), edited_days={})
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Понедельник", callback_data="iday_Monday"),
         InlineKeyboardButton(text="📅 Вторник", callback_data="iday_Tuesday")],
        [InlineKeyboardButton(text="📅 Среда", callback_data="iday_Wednesday"),
         InlineKeyboardButton(text="📅 Четверг", callback_data="iday_Thursday")],
        [InlineKeyboardButton(text="📅 Пятница", callback_data="iday_Friday"),
         InlineKeyboardButton(text="📅 Суббота", callback_data="iday_Saturday")],
        [InlineKeyboardButton(text="✅ Сохранить расписание", callback_data="save_schedule")],
        [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="back_to_menu")]
    ])
    
    await callback.message.edit_text(
        "📋 Выберите день для редактирования:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.choosing_day)
    
    await callback.answer()

async def back_to_schedule_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    interactive_schedule = data.get("interactive_schedule", {})
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Понедельник", callback_data="iday_Monday"),
         InlineKeyboardButton(text="📅 Вторник", callback_data="iday_Tuesday")],
        [InlineKeyboardButton(text="📅 Среда", callback_data="iday_Wednesday"),
         InlineKeyboardButton(text="📅 Четверг", callback_data="iday_Thursday")],
        [InlineKeyboardButton(text="📅 Пятница", callback_data="iday_Friday"),
         InlineKeyboardButton(text="📅 Суббота", callback_data="iday_Saturday")],
        [InlineKeyboardButton(text="✅ Сохранить расписание", callback_data="save_schedule")],
        [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="back_to_menu")]
    ])
    
    await callback.message.edit_text(
        format_schedule_for_preview(interactive_schedule),
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    await state.set_state(InteractiveScheduleStates.choosing_day)
    
    await callback.answer()

# ============================================================================
# HTTP ОБРАБОТЧИКИ
# ============================================================================

async def health_handler(request):
    return web.json_response({"status": "ok", "service": "tutorbot", "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat()})

async def root_handler(request):
    return web.Response(text="Bot is running!", status=200)

async def run_http_server():
    try:
        print("🌐 Creating HTTP application...")
        app = web.Application()
        app.router.add_get('/', root_handler)
        app.router.add_get('/health', health_handler)
        print("✅ HTTP application created")
        
        print(f"🌐 Starting HTTP server on 0.0.0.0:{PORT}...")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        
        print(f"✅ HTTP server started on 0.0.0.0:{PORT}")
        print("=" * 70)
        print("BOT IS READY")
        print("=" * 70)
        
        sys.stdout.flush()
        
        await asyncio.sleep(float('inf'))
    
    except Exception as e:
        print(f"❌ ERROR: HTTP server error: {e}")
        import traceback
        traceback.print_exc()

async def keep_alive_task():
    """ping keep-alive link every 14 minutes"""
    if not RENDER_URL:
        return
    
    await asyncio.sleep(30)
    
    while True:
        try:
            await asyncio.sleep(840)
            async with ClientSession() as session:
                try:
                    async with session.get(f'{RENDER_URL}/health', timeout=5) as resp:
                        if resp.status == 200:
                            print(f"✅ Keep-alive ping: {datetime.now(tz=MSK_TIMEZONE).strftime('%H:%M:%S')}")
                except Exception as e:
                    print(f"⚠️ Keep-alive: {e}")
        except Exception as e:
            print(f"⚠️ Keep-alive task error: {e}")
            await asyncio.sleep(60)

# ============================================================================
# ОСНОВНОЙ БОТ
# ============================================================================

async def start_bot():
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        try:
            print("🤖 Initializing Telegram bot...")
            print("🔧 Creating bot...")
            
            bot = Bot(token=TOKEN)
            storage = MemoryStorage()
            dp = Dispatcher(storage=storage)
            
            print("✅ Dispatcher created")
            print("📝 Registering handlers...")
            
            # Сообщения
            dp.message.register(start_handler, Command("start"))
            dp.message.register(menu_button_handler, F.text == "☰ Меню")
            dp.message.register(first_lesson_name_handler, FirstLessonStates.waiting_for_name)
            dp.message.register(first_lesson_class_handler, FirstLessonStates.waiting_for_class)
            dp.message.register(interactive_time_input_handler, InteractiveScheduleStates.waiting_for_start_time)
            dp.message.register(broadcast_text_handler, BroadcastMessageStates.waiting_for_message)
            
            # Callback запросы
            dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")
            dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")
            dp.callback_query.register(reschedule_lesson_handler, F.data == "reschedule_lesson")
            dp.callback_query.register(cancel_lesson_handler, F.data == "cancel_lesson")
            dp.callback_query.register(my_schedule_handler, F.data == "my_schedule")
            dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")
            dp.callback_query.register(subject_single_handler, F.data.startswith("subject_single_"))
            dp.callback_query.register(time_select_handler, F.data.startswith("time_"), FirstLessonStates.waiting_for_time)
            dp.callback_query.register(confirm_time_handler, F.data.startswith("confirm_time_"))
            dp.callback_query.register(repeat_time_select_handler, F.data.startswith("repeat_time_"), RepeatLessonStates.waiting_for_time)
            dp.callback_query.register(repeat_confirm_handler, F.data.startswith("repeat_confirm_"))
            dp.callback_query.register(reschedule_pick_handler, F.data.startswith("reschedule_pick_"), RescheduleStates.choosing_lesson)
            dp.callback_query.register(reschedule_day_handler, F.data.startswith("reschedule_day_"))
            dp.callback_query.register(reschedule_confirm_handler, F.data.startswith("reschedule_confirm_"))
            dp.callback_query.register(cancel_pick_handler, F.data.startswith("cancel_pick_"), CancelLessonStates.choosing_lesson)
            dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
            dp.callback_query.register(interactive_day_select_handler, F.data.startswith("iday_"))
            dp.callback_query.register(interactive_save_handler, F.data == "save_schedule")
            dp.callback_query.register(back_to_schedule_menu_handler, F.data == "back_to_schedule_menu")
            dp.callback_query.register(confirm_reschedule_handler, F.data.startswith("confirm_reschedule_"))
            dp.callback_query.register(reject_reschedule_handler, F.data.startswith("reject_reschedule_"))
            dp.callback_query.register(confirm_cancel_handler, F.data.startswith("confirm_cancel_"))
            dp.callback_query.register(reject_cancel_handler, F.data.startswith("reject_cancel_"))
            dp.callback_query.register(confirm_request_handler, F.data.startswith("confirm_") & ~F.data.startswith("confirm_reschedule_") & ~F.data.startswith("confirm_cancel_"))
            dp.callback_query.register(reject_request_handler, F.data.startswith("reject_") & ~F.data.startswith("reject_reschedule_") & ~F.data.startswith("reject_cancel_"))
            
            # ✅ ИСПРАВЛЕНО: Обработчики для просьбы о переносе от репетитора
            dp.callback_query.register(tutor_reschedule_request_handler, F.data == "tutor_reschedule_request")
            dp.callback_query.register(tutor_reschedule_pick_handler, F.data.startswith("tutor_reschedule_pick_"))
            dp.callback_query.register(tutor_reschedule_day_handler, F.data.startswith("tutor_reschedule_day_"))
            dp.callback_query.register(tutor_reschedule_confirm_handler, F.data.startswith("tutor_reschedule_confirm_"))
            dp.callback_query.register(student_reschedule_agree_handler, F.data.startswith("student_reschedule_agree_"))
            dp.callback_query.register(student_reschedule_decline_handler, F.data.startswith("student_reschedule_decline_"))
            
            # ✅ ИСПРАВЛЕНО: Обработчик для отправки сообщения всем
            dp.callback_query.register(broadcast_message_handler, F.data == "broadcast_message")
            
            print("✅ Handlers registered")
            print("⏳ Waiting for messages from Telegram...")
            sys.stdout.flush()
            
            retry_count = 0
            
            # Запускаем фоновые задачи
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
                print(f"⚠️ Telegram: Conflict! Повтор {retry_count}/{max_retries} через {wait_time} сек...")
                sys.stdout.flush()
                await asyncio.sleep(wait_time)
                continue
            
            print(f"❌ ERROR: Bot error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)
            continue
    
    if retry_count >= max_retries:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {max_retries} попыток не удалось подключиться")
        sys.exit(1)

async def main():
    print("=" * 70)
    print("INITIALIZING APPLICATION - FIXED TIMEZONE SUPPORT + ALL IMPROVEMENTS")
    print("=" * 70)
    print()
    
    print(f"🔌 Port: {PORT}")
    print(f"🔐 Token: {'✅ OK' if TOKEN else '❌ NOT SET'}")
    print(f"🌐 Render URL: {RENDER_URL if RENDER_URL else '❌ NOT SET'}")
    print(f"⏰ Max work hour: {MAX_WORK_HOUR}:00")
    print(f"⏳ Slot duration: {SLOT_DURATION} minutes")
    print(f"🕐 Timezone: MSK (UTC+3)")
    print("=" * 70)
    sys.stdout.flush()
    
    lockfile = Path('./.botrunning.lock')
    
    if lockfile.exists():
        print("⚠️ Обнаружена старая блокировка. Попытка удаления...")
        try:
            lockfile.unlink()
        except Exception as e:
            print(f"⚠️ Warning: Could not delete old lock file: {e}")
    
    lockfile.write_text(str(os.getpid()))
    print(f"✅ Lock file created: {lockfile}")
    
    print("\n🧹 Performing startup cleanup...")
    cleanup_stale_requests()
    
    # ✅ НОВОЕ: Восстанавливаем STUDENT_CACHE из файлов
    restore_cache_from_files()
    
    print(f"✅ STUDENT_CACHE восстановлен: {len(STUDENT_CACHE)} записей")
    
    # Показываем содержимое файлов для отладки
    print(f"📊 Расписание: {load_json(SCHEDULE_FILE)}")
    print(f"📊 Подтвержденные занятия: {len(load_json(CONFIRMED_FILE))} записей")
    print(f"📊 Ученики в students.json: {len(load_json(STUDENTS_FILE))} записей")
    
    SENT_REMINDERS.clear()
    print("🧹 Очищены старые напоминания при запуске")
    print("✅ Startup cleanup completed\n")
    
    sys.stdout.flush()
    
    try:
        await asyncio.gather(
            run_http_server(),
            start_bot()
        )
    except KeyboardInterrupt:
        print("⏸ Application interrupted by user")
    except Exception as e:
        print(f"❌ ERROR: Main thread error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if lockfile.exists():
            try:
                lockfile.unlink()
            except:
                pass
        
        print("✅ Bot stopped correctly")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Bot stopped")
    except Exception as e:
        print(f"❌ ERROR: Main thread error: {e}")
        import traceback
        traceback.print_exc()
