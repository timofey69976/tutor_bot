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
    waiting_for_subjects = State()
    waiting_for_time = State()

class RepeatLessonStates(StatesGroup):
    waiting_for_subjects = State()
    waiting_for_time = State()

class TutorScheduleStates(StatesGroup):
    waiting_for_schedule_json = State()

class RescheduleStates(StatesGroup):
    choosing_lesson = State()
    waiting_for_new_time = State()
    waiting_for_confirmation = State()

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

def subjects_keyboard(selected=None):
    if selected is None:
        selected = []
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'✅' if s in selected else '⬜'} {s}", callback_data=f"subject_{s}")
         for s in SUBJECTS],
        [InlineKeyboardButton(text="✓ Подтвердить выбор", callback_data="subjects_done")]
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
        "Выбери предметы, которыми ты будешь заниматься:",
        reply_markup=subjects_keyboard()
    )
    await state.set_state(FirstLessonStates.waiting_for_subjects)

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
        "Выбери предметы, которыми ты занимаешься:",
        reply_markup=subjects_keyboard()
    )
    await state.set_state(RepeatLessonStates.waiting_for_subjects)

async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text(
        "👋 Добро пожаловать! Выбери, что тебе нужно:",
        reply_markup=main_menu_keyboard(callback.from_user.id)
    )

async def subject_select_handler(callback: types.CallbackQuery, state: FSMContext):
    subject = callback.data.replace("subject_", "")
    data = await state.get_data()
    selected = data.get("selected_subjects", [])
    if subject in selected:
        selected.remove(subject)
    else:
        selected.append(subject)
    await state.update_data(selected_subjects=selected)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'✅' if s in selected else '⬜'} {s}", callback_data=f"subject_{s}") 
         for s in SUBJECTS],
        [InlineKeyboardButton(text="✓ Подтвердить выбор", callback_data="subjects_done")]
    ])
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer(f"{'Добавлен' if subject in selected else 'Убран'}: {subject}")

async def subjects_done_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get("selected_subjects", [])
    if not selected:
        await callback.answer("Выбери хотя бы один предмет!", show_alert=True)
        return
    await callback.answer()
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
    if await state.get_state() == FirstLessonStates.waiting_for_subjects:
        await state.set_state(FirstLessonStates.waiting_for_time)
    else:
        await state.set_state(RepeatLessonStates.waiting_for_time)

async def time_select_handler(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace("time_", "").split("_", 1)
    day_name = parts[0]
    time_slot = parts[1]
    
    data = await state.get_data()
    selected_subjects = ", ".join(data.get("selected_subjects", []))
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
        f"📚 Предмет(ы): {selected_subjects}\n"
        f"📅 {date_str}\n"
        f"🕐 {time_slot}\n\n"
        f"⏳ Ожидается подтверждение от репетитора..."
    )
    
    await callback.message.edit_text(student_message, reply_markup=None)
    
    tutor_message = (
        f"📬 НОВЫЙ ЗАПРОС НА ЗАНЯТИЕ\n\n"
        f"👤 Ученик: {student_name}\n"
        f"📖 Класс: {student_grade}\n"
        f"📚 Предметы: {selected_subjects}\n"
        f"📅 {date_str}\n"
        f"🕐 {time_slot}\n"
    )
    
    request_id = create_request_id()
    pending = load_json(PENDING_FILE)
    pending[request_id] = {
        "student_id": callback.from_user.id,
        "student_name": student_name,
        "grade": student_grade,
        "subjects": data.get('selected_subjects', []),
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
        print(f"❌ Ошибка: {e}")
    
    await callback.answer()
    await state.clear()

async def tutor_confirm_handler(callback: types.CallbackQuery):
    request_id = callback.data.replace("confirm_", "")
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("❌ Запрос не найден", show_alert=True)
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
        callback.message.text + "\n\n✅ *Занятие подтверждено и добавлено в расписание*",
        parse_mode="Markdown"
    )
    await callback.answer("✅ Занятие подтверждено!")

async def tutor_reject_handler(callback: types.CallbackQuery):
    request_id = callback.data.replace("reject_", "")
    pending = load_json(PENDING_FILE)
    if request_id not in pending:
        await callback.answer("❌ Запрос не найден", show_alert=True)
        return
    
    request = pending[request_id]
    student_id = request["student_id"]
    
    student_message = (
        f"❌ К сожалению, в выбранное время занятие провести не получится.\n\n"
        f"Попробуй выбрать другое время или день."
    )
    
    try:
        await callback.bot.send_message(student_id, student_message)
    except:
        pass
    
    request["status"] = "rejected"
    pending[request_id] = request
    save_json(PENDING_FILE, pending)
    
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Занятие отклонено*",
        parse_mode="Markdown"
    )
    await callback.answer("❌ Запрос отклонен")

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

# ============================================================================
# HTTP СЕРВЕР
# ============================================================================

async def health_handler(request):
    return web.json_response({"status": "ok", "service": "tutor_bot"})

async def root_handler(request):
    return web.Response(text="🤖 Telegram бот работает!", status=200)

async def run_http_server():
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
        print("=" * 70)
        print("🤖 БОТ ГОТОВ К РАБОТЕ")
        print("=" * 70)
        sys.stdout.flush()
        
        await asyncio.sleep(float('inf'))
    except Exception as e:
        print(f"❌ Ошибка HTTP сервера: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# БОТ
# ============================================================================

def run_bot():
    try:
        print("\n⏳ Инициализация Telegram бота...")
        
        bot_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(bot_loop)
        
        async def bot_main():
            try:
                print("⏳ Создание бота...")
                bot = Bot(token=TOKEN)
                storage = MemoryStorage()
                dp = Dispatcher(storage=storage)
                print("✅ Диспетчер создан")
                
                print("⏳ Регистрация обработчиков...")
                
                dp.message.register(start_handler, Command("start"))
                dp.message.register(menu_button_handler, F.text == "☰ Меню")
                dp.message.register(first_lesson_name_handler, FirstLessonStates.waiting_for_name)
                dp.message.register(first_lesson_class_handler, FirstLessonStates.waiting_for_class)
                dp.message.register(schedule_json_handler, TutorScheduleStates.waiting_for_schedule_json)
                
                dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")
                dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")
                dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")
                dp.callback_query.register(subject_select_handler, F.data.startswith("subject_"))
                dp.callback_query.register(subjects_done_handler, F.data == "subjects_done")
                dp.callback_query.register(time_select_handler, F.data.startswith("time_"))
                dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")
                dp.callback_query.register(tutor_confirm_handler, F.data.startswith("confirm_"))
                dp.callback_query.register(tutor_reject_handler, F.data.startswith("reject_"))
                
                print("✅ Обработчики зарегистрированы")
                print("⏳ Ожидание сообщений от Telegram...\n")
                sys.stdout.flush()
                
                # Запускаем напоминания в фоне
                reminder_task = asyncio.create_task(send_reminders(bot))
                try:
                    await dp.start_polling(bot, skip_updates=True, handle_signals=False)
                finally:
                    reminder_task.cancel()
                
            except Exception as e:
                print(f"❌ Ошибка бота: {e}")
                import traceback
                traceback.print_exc()
        
        bot_loop.run_until_complete(bot_main())
        
    except Exception as e:
        print(f"❌ Критическая ошибка бота: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    try:
        print("=" * 70)
        print("🚀 ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ")
        print("=" * 70)
        print(f"📌 Порт: {PORT}")
        print(f"🔑 Токен: {'✅ Установлен' if TOKEN else '⚠️  НЕ установлен'}")
        print("=" * 70)
        print("⏳ Запуск HTTP сервера в главном потоке...\n")
        sys.stdout.flush()
        
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        
        asyncio.run(run_http_server())
        
    except KeyboardInterrupt:
        print("\n⏹️  Приложение остановлено")
    except Exception as e:
        print(f"❌ Ошибка главного потока: {e}")
        import traceback
        traceback.print_exc()

