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
from aiogram.client.session.aiohttp import AiohttpSession

# ============================================================================
# КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# ============================================================================

PORT = int(os.getenv('PORT', 10000))
TOKEN = os.getenv('TOKEN')
RENDER_URL = os.getenv('RENDER_URL', '')

if not TOKEN:
    TOKEN = '8388119061:AAEfeIhBSsD_3WyVS3L_YRtdbvbQxyf5RCM'

TUTOR_ID = 1339816111

# --- НАСТРОЙКА СЕТИ ---
from aiohttp import ClientTimeout

timeout_settings = ClientTimeout(total=45, connect=10, sock_read=45)
session = AiohttpSession(timeout=timeout_settings)

bot = Bot(token=TOKEN, session=session)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)  # ✅ ЕДИНСТВЕННЫЙ диспетчер

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

MSK_TIMEZONE = timezone(timedelta(hours=3))

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
MESSAGE_LOG_FILE = DATA_DIR / "message_log.json"

print(f"📝 Files will be saved to:")
print(f" - {STUDENTS_FILE}")
print(f" - {SCHEDULE_FILE}")
print(f" - {CONFIRMED_FILE}")
print(f" - {PENDING_FILE}\n")

STUDENT_CACHE = {}
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

def restore_cache_from_files():
    """Восстановить STUDENT_CACHE из всех файлов при запуске"""
    global STUDENT_CACHE
    
    print("🔄 Восстанавливаю STUDENT_CACHE из файлов...")
    
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
    
    confirmed_data = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed_data.items():
        student_id = lesson.get("student_id")
        if student_id and student_id != TUTOR_ID and student_id not in STUDENT_CACHE:
            STUDENT_CACHE[student_id] = {
                "name": lesson.get("student_name", ""),
                "grade": lesson.get("student_class", "")
            }
    
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
    """Отправлять напоминание за 60 минут до занятия"""
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
                    
                    if 3480 <= time_diff <= 3720:
                        reminder_key = f"{lesson_id}:{lesson_time.isoformat()}"
                        
                        if reminder_key not in SENT_REMINDERS:
                            student_id = lesson.get('student_id')
                            student_name = lesson.get('student_name')
                            subject = lesson.get('subject')
                            lesson_time_str = lesson_time.strftime('%H:%M')
                            
                            print(f"📤 Отправляю напоминание для занятия {lesson_id}")
                            
                            msg_student = await bot.send_message(
                                student_id,
                                f"⏰ Напоминание о занятии!\n\n"
                                f"Предмет: {subject}\n"
                                f"Время: {lesson_time_str}\n\n"
                                f"Занятие начинается через 1 час! 📚",
                                parse_mode="HTML",
                                reply_markup=persistent_menu_keyboard()
                            )
                            log_message(student_id, msg_student.message_id)
                            
                            msg_tutor = await bot.send_message(
                                TUTOR_ID,
                                f"⏰ Напоминание о занятии!\n\n"
                                f"Ученик: {student_name}\n"
                                f"Предмет: {subject}\n"
                                f"Время: {lesson_time_str}\n\n"
                                f"Занятие начинается через 1 час! 📚",
                                parse_mode="HTML",
                                reply_markup=persistent_menu_keyboard()
                            )
                            log_message(TUTOR_ID, msg_tutor.message_id)
                            
                            SENT_REMINDERS.add(reminder_key)
                            print(f"✅ Напоминание отправлено и запомнено")
                        else:
                            print(f"⏭️ Напоминание для {lesson_id} уже отправлено, пропускаем")
                
                except Exception as e:
                    print(f"⚠️ Ошибка при обработке напоминания {lesson_id}: {e}")
            
            await asyncio.sleep(60)
            
            if int(now.timestamp()) % 600 == 0:
                cleanup_sent_reminders_list()
        
        except Exception as e:
            print(f"⚠️ Ошибка в send_reminders: {e}")
            await asyncio.sleep(60)

async def send_daily_schedule(bot: Bot):
    """Отправлять ежедневное расписание репетитору в 8:00"""
    await asyncio.sleep(120)
    
    while True:
        try:
            now = datetime.now(tz=MSK_TIMEZONE)
            
            if now.hour == 8 and 0 <= now.minute < 5:
                print(f"📅 Отправляю расписание на сегодня в {now.strftime('%H:%M:%S')}")
                
                all_lessons = load_json(CONFIRMED_FILE)
                today_date = now.date()
                today_lessons = []
                
                for lesson_id, lesson in all_lessons.items():
                    try:
                        lesson_datetime = datetime.fromisoformat(lesson.get("lesson_datetime", ""))
                        
                        if lesson_datetime.tzinfo is None:
                            lesson_datetime = lesson_datetime.replace(tzinfo=MSK_TIMEZONE)
                        
                        if lesson_datetime.date() == today_date:
                            today_lessons.append((lesson_datetime, lesson))
                    except Exception as e:
                        print(f"⚠️ Ошибка при обработке занятия {lesson_id}: {e}")
                
                today_lessons.sort(key=lambda x: x[0])
                
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
                
                msg = await bot.send_message(
                    TUTOR_ID,
                    message,
                    parse_mode="HTML",
                    reply_markup=persistent_menu_keyboard()
                )
                log_message(TUTOR_ID, msg.message_id)
                
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

def log_message(chat_id: int, message_id: int, message_type: str = "bot"):
    """Записать сообщение в лог для последующего удаления"""
    message_log = load_json(MESSAGE_LOG_FILE)
    
    message_key = f"{chat_id}_{message_id}"
    message_log[message_key] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "type": message_type,
        "timestamp": datetime.now(tz=MSK_TIMEZONE).isoformat()
    }
    
    save_json(MESSAGE_LOG_FILE, message_log)
    print(f"📝 Записано сообщение {message_id} для чата {chat_id}")

async def delete_old_messages(bot: Bot):
    """Удалять старые сообщения (старше 24 часов)"""
    await asyncio.sleep(600)
    
    while True:
        try:
            now = datetime.now(tz=MSK_TIMEZONE)
            message_log = load_json(MESSAGE_LOG_FILE)
            
            if not message_log:
                await asyncio.sleep(3600)
                continue
            
            deleted_count = 0
            messages_to_delete = []
            
            for message_key, message_info in message_log.items():
                try:
                    msg_time = datetime.fromisoformat(message_info.get("timestamp", ""))
                    if msg_time.tzinfo is None:
                        msg_time = msg_time.replace(tzinfo=MSK_TIMEZONE)
                    
                    if (now - msg_time).total_seconds() > 86400:
                        messages_to_delete.append((message_key, message_info))
                except Exception as e:
                    print(f"⚠️ Ошибка при обработке сообщения {message_key}: {e}")
            
            for message_key, message_info in messages_to_delete:
                try:
                    chat_id = message_info.get("chat_id")
                    message_id = message_info.get("message_id")
                    
                    if chat_id and message_id:
                        await bot.delete_message(chat_id=chat_id, message_id=message_id)
                        print(f"🗑️ Удалено сообщение {message_id} из чата {chat_id}")
                        deleted_count += 1
                    
                    del message_log[message_key]
                
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение {message_key}: {e}")
                    if message_key in message_log:
                        del message_log[message_key]
            
            if messages_to_delete:
                save_json(MESSAGE_LOG_FILE, message_log)
                print(f"✅ Удалено {deleted_count} старых сообщений")
            
            await asyncio.sleep(3600)
        
        except Exception as e:
            print(f"⚠️ Ошибка в delete_old_messages: {e}")
            await asyncio.sleep(3600)

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
        kb.inline_keyboard.append(
            [InlineKeyboardButton(text="📬 Просьба о переносе", callback_data="tutor_reschedule_request")]
        )
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
    """Получить даты текущей недели (понедельник - суббота)"""
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
    """Получить доступные времена для дня"""
    all_times = schedule.get(day_name, [])
    
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
    """Получить всех, кто хотя бы раз пользовался ботом"""
    all_students = {}
    
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
    
    confirmed = load_json(CONFIRMED_FILE)
    for lesson_id, lesson in confirmed.items():
        student_id = lesson.get("student_id")
        if student_id and student_id not in all_students and student_id != TUTOR_ID:
            all_students[student_id] = {
                "name": lesson.get("student_name", "Ученик"),
                "class": lesson.get("student_class", "")
            }
    
    pending = load_json(PENDING_FILE)
    for req_id, req in pending.items():
        student_id = req.get("student_id")
        if student_id and student_id not in all_students and student_id != TUTOR_ID:
            all_students[student_id] = {
                "name": req.get("student_name", "Ученик"),
                "class": req.get("student_class", "")
            }
    
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
