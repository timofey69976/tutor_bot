"""

Telegram бот для управления расписанием занятий репетитора

Работает локально на компьютере репетитора

"""

import asyncio

import json

from datetime import datetime, timedelta

from pathlib import Path

from typing import Optional, Dict, List, Tuple

from aiogram import Bot, Dispatcher, types, F

from aiogram.fsm.context import FSMContext

from aiogram.fsm.state import State, StatesGroup

from aiogram.fsm.storage.memory import MemoryStorage

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

from aiogram.filters import Command

# ============================================================================

# КОНФИГУРАЦИЯ

# ============================================================================

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

# ХРАНИЛИЩЕ ДАННЫХ (JSON файлы)

# ============================================================================

DATA_DIR = Path("bot_data")

DATA_DIR.mkdir(exist_ok=True)

STUDENTS_FILE = DATA_DIR / "students.json"

SCHEDULE_FILE = DATA_DIR / "schedule.json"

PENDING_FILE = DATA_DIR / "pending_requests.json"

CONFIRMED_FILE = DATA_DIR / "confirmed_lessons.json"

def load_json(filepath):

    """Загрузить JSON файл"""

    if filepath.exists():

        with open(filepath, 'r', encoding='utf-8') as f:

            return json.load(f)

    return {}

def save_json(filepath, data):

    """Сохранить JSON файл"""

    with open(filepath, 'w', encoding='utf-8') as f:

        json.dump(data, f, ensure_ascii=False, indent=2)

# ============================================================================

# СОСТОЯНИЯ (FSM - Finite State Machine)

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

    """Главное меню для ученика/репетитора"""

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

    """Быстрое меню с четырьмя кнопками"""

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="📍 Перенести занятие", callback_data="reschedule_lesson")],

        [InlineKeyboardButton(text="❌ Отменить занятие", callback_data="cancel_lesson")],

        [InlineKeyboardButton(text="📅 Повторное занятие", callback_data="repeat_lesson")],

        [InlineKeyboardButton(text="🏠 Вернуться в начало", callback_data="back_to_menu")]

    ])

    return kb

def persistent_menu_keyboard():

    """Постоянная кнопка меню в поле ввода"""

    kb = ReplyKeyboardMarkup(keyboard=[

        [KeyboardButton(text="☰ Меню")]

    ], resize_keyboard=True, one_time_keyboard=False)

    return kb

def subjects_keyboard(multiple=True):

    """Клавиатура выбора предметов"""

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text=f"{'✅' if multiple else ''} {subject}", callback_data=f"subject_{subject}")

        for subject in SUBJECTS],

        [InlineKeyboardButton(text="✓ Подтвердить выбор", callback_data="subjects_done")]

    ])

    return kb

def tutor_confirm_keyboard(request_id: str):

    """Клавиатура подтверждения для репетитора"""

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{request_id}")],

        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{request_id}")]

    ])

    return kb

def tutor_reschedule_confirm_keyboard(reschedule_id: str):

    """Клавиатура подтверждения переноса для репетитора"""

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="✅ Подтвердить перенос", callback_data=f"confirm_reschedule_{reschedule_id}")],

        [InlineKeyboardButton(text="❌ Отклонить перенос", callback_data=f"reject_reschedule_{reschedule_id}")]

    ])

    return kb

def tutor_cancel_confirm_keyboard(cancel_id: str):

    """Клавиатура подтверждения отмены для репетитора"""

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="✅ Подтвердить отмену", callback_data=f"confirm_cancel_tutor_{cancel_id}")],

        [InlineKeyboardButton(text="❌ Оставить занятие", callback_data=f"reject_cancel_{cancel_id}")]

    ])

    return kb

def lessons_list_keyboard(lessons: Dict, action_type: str = "reschedule"):

    """Клавиатура списка занятий"""

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

def get_week_dates(start_date: datetime = None) -> Dict[str, Tuple[datetime, str]]:

    """

    Получить даты на неделю (пн-сб)

    Возвращает: {day_name: (date, formatted_date)}

    """

    if start_date is None:

        start_date = datetime.now()

    days_ahead = 0 - start_date.weekday()

    if days_ahead <= 0:

        days_ahead += 7

    week_start = start_date + timedelta(days=days_ahead)

    days_map = {

        0: "Monday",

        1: "Tuesday",

        2: "Wednesday",

        3: "Thursday",

        4: "Friday",

        5: "Saturday"

    }

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

    """Получить доступное время для выбранного дня"""

    return schedule.get(day_name, [])

def create_request_id():

    """Создать уникальный ID для запроса"""

    import uuid

    return str(uuid.uuid4())[:8]

def parse_time(time_str: str) -> tuple:

    """Преобразовать строку времени в часы и минуты"""

    parts = time_str.split(":")

    return int(parts[0]), int(parts[1])

def get_lesson_datetime(day_name: str, time_str: str) -> Optional[datetime]:

    """Получить объект datetime для урока"""

    week = get_week_dates()

    if day_name not in week:

        return None

    date_obj, _ = week[day_name]

    hour, minute = parse_time(time_str)

    return date_obj.replace(hour=hour, minute=minute, second=0, microsecond=0)

def get_student_lessons(student_id: int) -> Dict:

    """Получить все подтвержденные занятия ученика"""

    confirmed = load_json(CONFIRMED_FILE)

    student_lessons = {}

    for lesson_id, lesson in confirmed.items():

        if lesson["student_id"] == student_id:

            student_lessons[lesson_id] = lesson

    return student_lessons

def get_student_info(student_id: int) -> Optional[Dict]:

    """Получить информацию об ученике (имя и класс)"""

    students = load_json(STUDENTS_FILE)

    return students.get(str(student_id))

def save_student_info(student_id: int, name: str, grade: str):

    """Сохранить информацию об ученике"""

    students = load_json(STUDENTS_FILE)

    students[str(student_id)] = {"name": name, "grade": grade}

    save_json(STUDENTS_FILE, students)

# ============================================================================

# ОБРАБОТЧИКИ (Handlers)

# ============================================================================

async def start_handler(message: types.Message, state: FSMContext):

    """Команда /start"""

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

    """Обработка кнопки меню из поля ввода"""

    if message.text == "☰ Меню":

        await state.clear()

        await message.answer(

            "📋 Быстрое меню:",

            reply_markup=quick_menu_keyboard()

        )

        return True

    return False

# ============ ПЕРВОЕ ЗАНЯТИЕ ============

async def first_lesson_handler(callback: types.CallbackQuery, state: FSMContext):

    """Нажата кнопка 'Первое занятие'"""

    await callback.answer()

    await callback.message.edit_text(

        "Введи своё имя:"

    )

    await state.set_state(FirstLessonStates.waiting_for_name)

async def first_lesson_name_handler(message: types.Message, state: FSMContext):

    """Получить имя ученика"""

    if await menu_button_handler(message, state):

        return

    print(f"DEBUG: Получено имя: {message.text}")

    await state.update_data(name=message.text)

    await message.answer(

        "Введи номер класса (например, 9 или 10):"

    )

    await state.set_state(FirstLessonStates.waiting_for_class)

async def first_lesson_class_handler(message: types.Message, state: FSMContext):

    """Получить класс ученика"""

    if await menu_button_handler(message, state):

        return

    print(f"DEBUG: Получен класс: {message.text}")

    await state.update_data(grade=message.text)

    data = await state.get_data()

    save_student_info(message.from_user.id, data["name"], message.text)

    print(f"DEBUG: Сохранен студент: {data['name']}, класс {message.text}")

    await message.answer(

        "Выбери предметы, которыми ты будешь заниматься:",

        reply_markup=subjects_keyboard()

    )

    await state.set_state(FirstLessonStates.waiting_for_subjects)

async def repeat_lesson_handler(callback: types.CallbackQuery, state: FSMContext):

    """Нажата кнопка 'Повторное занятие'"""

    await callback.answer()

    student_info = get_student_info(callback.from_user.id)

    if not student_info:

        await callback.message.edit_text(

            "❌ Ты ещё не записывался на первое занятие.\n\n"

            "Пожалуйста, сначала запишись на первое занятие.",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]

            ])

        )

        return

    await state.update_data(

        name=student_info["name"],

        grade=student_info["grade"]

    )

    await callback.message.edit_text(

        "Выбери предметы, которыми ты занимаешься:",

        reply_markup=subjects_keyboard()

    )

    await state.set_state(RepeatLessonStates.waiting_for_subjects)

async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):

    """Вернуться в главное меню"""

    await callback.answer()

    await state.clear()

    await callback.message.edit_text(

        "👋 Добро пожаловать! Выбери, что тебе нужно:",

        reply_markup=main_menu_keyboard(callback.from_user.id)

    )

async def subject_select_handler(callback: types.CallbackQuery, state: FSMContext):

    """Выбор предмета (с поддержкой множественного выбора)"""

    subject = callback.data.replace("subject_", "")

    data = await state.get_data()

    selected = data.get("selected_subjects", [])

    if subject in selected:

        selected.remove(subject)

    else:

        selected.append(subject)

    await state.update_data(selected_subjects=selected)

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(

            text=f"{'✅' if s in selected else '⬜'} {s}",

            callback_data=f"subject_{s}"

        ) for s in SUBJECTS],

        [InlineKeyboardButton(text="✓ Подтвердить выбор", callback_data="subjects_done")]

    ])

    await callback.message.edit_reply_markup(reply_markup=kb)

    await callback.answer(f"{'Добавлен' if subject in selected else 'Убран'}: {subject}")

async def subjects_done_handler(callback: types.CallbackQuery, state: FSMContext):

    """Завершить выбор предметов и показать время"""

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

    """Выбор времени и дня занятия"""

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

        print(f"Ошибка при отправке сообщения репетитору: {e}")

    await callback.answer()

    await state.clear()

# ============ ПЕРЕНОС ЗАНЯТИЯ ============

async def reschedule_lesson_handler(callback: types.CallbackQuery, state: FSMContext):

    """Нажата кнопка 'Перенести занятие'"""

    await callback.answer()

    student_lessons = get_student_lessons(callback.from_user.id)

    if not student_lessons:

        await callback.message.edit_text(

            "❌ У тебя нет подтвержденных занятий для переноса.",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]

            ])

        )

        return

    await callback.message.edit_text(

        "📅 Выбери занятие для переноса:",

        reply_markup=lessons_list_keyboard(student_lessons, "select_reschedule")

    )

    await state.set_state(RescheduleStates.choosing_lesson)

async def select_reschedule_handler(callback: types.CallbackQuery, state: FSMContext):

    """Выбор занятия для переноса"""

    lesson_id = callback.data.replace("select_reschedule_", "")

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id not in confirmed:

        await callback.answer("❌ Занятие не найдено", show_alert=True)

        return

    lesson = confirmed[lesson_id]

    subjects_str = ", ".join(lesson["subjects"])

    await callback.answer()

    await state.update_data(reschedule_lesson_id=lesson_id)

    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE

    week = get_week_dates()

    message_text = f"📅 Старое время: {lesson['date_str']} {lesson['time']}\n\n"

    message_text += "Выбери новый день и время:\n\n"

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

                    callback_data=f"newtime_{day_name}_{time}"

                )

            ])

    kb.inline_keyboard.append([

        InlineKeyboardButton(text="⬅️ Вернуться", callback_data="back_to_menu")

    ])

    await callback.message.edit_text(message_text, reply_markup=kb, parse_mode="Markdown")

    await state.set_state(RescheduleStates.waiting_for_new_time)

async def select_new_time_handler(callback: types.CallbackQuery, state: FSMContext):

    """Выбор нового времени для переноса"""

    parts = callback.data.replace("newtime_", "").split("_", 1)

    new_day = parts[0]

    new_time = parts[1]

    data = await state.get_data()

    lesson_id = data.get("reschedule_lesson_id")

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id not in confirmed:

        await callback.answer("❌ Занятие не найдено", show_alert=True)

        return

    lesson = confirmed[lesson_id]

    subjects_str = ", ".join(lesson["subjects"])

    week = get_week_dates()

    new_date_str = week[new_day][1]

    reschedule_id = create_request_id()

    await state.update_data(

        reschedule_id=reschedule_id,

        new_day=new_day,

        new_time=new_time,

        new_date_str=new_date_str

    )

    student_confirmation = (

        f"⏳ ЗАПРОС НА ПЕРЕНОС ЗАНЯТИЯ\n\n"

        f"📚 {subjects_str}\n"

        f"📅 Старое время: {lesson['date_str']} {lesson['time']}\n"

        f"📅 Новое время: {new_date_str} {new_time}\n\n"

        f"⏳ Ожидается подтверждение от репетитора..."

    )

    await callback.message.edit_text(student_confirmation, reply_markup=None)

    tutor_reschedule_message = (

        f"📍 ЗАПРОС НА ПЕРЕНОС ЗАНЯТИЯ\n\n"

        f"👤 Ученик: {lesson['student_name']}\n"

        f"📚 {subjects_str}\n"

        f"📅 Старое время: {lesson['date_str']} {lesson['time']}\n"

        f"📅 Новое время: {new_date_str} {new_time}\n"

    )

    try:

        await callback.bot.send_message(

            TUTOR_ID,

            tutor_reschedule_message,

            reply_markup=tutor_reschedule_confirm_keyboard(reschedule_id)

        )

    except Exception as e:

        print(f"Ошибка при отправке запроса переноса репетитору: {e}")

    pending_reschedules = load_json(DATA_DIR / "pending_reschedules.json")

    pending_reschedules[reschedule_id] = {

        "lesson_id": lesson_id,

        "student_id": lesson["student_id"],

        "student_name": lesson["student_name"],

        "old_day": lesson["day"],

        "old_time": lesson["time"],

        "old_date_str": lesson["date_str"],

        "new_day": new_day,

        "new_time": new_time,

        "new_date_str": new_date_str,

        "subjects": lesson["subjects"],

        "status": "pending",

        "timestamp": datetime.now().isoformat()

    }

    save_json(DATA_DIR / "pending_reschedules.json", pending_reschedules)

    await callback.answer()

    await state.set_state(RescheduleStates.waiting_for_confirmation)

async def tutor_confirm_reschedule_handler(callback: types.CallbackQuery):

    """Репетитор подтвердил перенос занятия"""

    reschedule_id = callback.data.replace("confirm_reschedule_", "")

    pending_reschedules = load_json(DATA_DIR / "pending_reschedules.json")

    if reschedule_id not in pending_reschedules:

        await callback.answer("❌ Запрос на перенос не найден", show_alert=True)

        return

    reschedule = pending_reschedules[reschedule_id]

    lesson_id = reschedule["lesson_id"]

    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE

    old_day = reschedule["old_day"]

    old_time = reschedule["old_time"]

    new_day = reschedule["new_day"]

    new_time = reschedule["new_time"]

    if old_time not in schedule[old_day]:

        schedule[old_day].append(old_time)

    schedule[old_day].sort()

    if new_time in schedule[new_day]:

        schedule[new_day].remove(new_time)

    save_json(SCHEDULE_FILE, schedule)

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id in confirmed:

        confirmed[lesson_id]["day"] = new_day

        confirmed[lesson_id]["time"] = new_time

        confirmed[lesson_id]["date_str"] = reschedule["new_date_str"]

        confirmed[lesson_id]["reminder_sent"] = False

    save_json(CONFIRMED_FILE, confirmed)

    del pending_reschedules[reschedule_id]

    save_json(DATA_DIR / "pending_reschedules.json", pending_reschedules)

    subjects_str = ", ".join(reschedule["subjects"])

    student_message = (

        f"✅ ПЕРЕНОС ПОДТВЕРЖДЕН!\n\n"

        f"📚 {subjects_str}\n"

        f"📅 Новое время: {reschedule['new_date_str']}\n"

        f"🕐 {reschedule['new_time']}\n"

    )

    await callback.bot.send_message(reschedule["student_id"], student_message)

    await callback.message.edit_text(

        callback.message.text + "\n\n✅ *Перенос подтвережен и добавлен в расписание*",

        parse_mode="Markdown"

    )

    await callback.answer("✅ Перенос подтвержден!")

async def tutor_reject_reschedule_handler(callback: types.CallbackQuery):

    """Репетитор отклонил перенос занятия"""

    reschedule_id = callback.data.replace("reject_reschedule_", "")

    pending_reschedules = load_json(DATA_DIR / "pending_reschedules.json")

    if reschedule_id not in pending_reschedules:

        await callback.answer("❌ Запрос на перенос не найден", show_alert=True)

        return

    reschedule = pending_reschedules[reschedule_id]

    del pending_reschedules[reschedule_id]

    save_json(DATA_DIR / "pending_reschedules.json", pending_reschedules)

    student_message = (

        f"❌ Перенос занятия отклонен.\n\n"

        f"Попробуй выбрать другое время или дату."

    )

    await callback.bot.send_message(reschedule["student_id"], student_message)

    await callback.message.edit_text(

        callback.message.text + "\n\n❌ *Перенос отклонен*",

        parse_mode="Markdown"

    )

    await callback.answer("❌ Перенос отклонен")

# ============ ОТМЕНА ЗАНЯТИЯ ============

async def cancel_lesson_handler(callback: types.CallbackQuery, state: FSMContext):

    """Нажата кнопка 'Отменить занятие'"""

    await callback.answer()

    student_lessons = get_student_lessons(callback.from_user.id)

    if not student_lessons:

        await callback.message.edit_text(

            "❌ У тебя нет подтвережденных занятий для отмены.",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]

            ])

        )

        return

    await callback.message.edit_text(

        "❌ Выбери занятие для отмены:",

        reply_markup=lessons_list_keyboard(student_lessons, "select_cancel")

    )

    await state.set_state(CancelLessonStates.choosing_lesson)

async def select_cancel_handler(callback: types.CallbackQuery, state: FSMContext):

    """Выбор занятия для отмены"""

    lesson_id = callback.data.replace("select_cancel_", "")

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id not in confirmed:

        await callback.answer("❌ Занятие не найдено", show_alert=True)

        return

    lesson = confirmed[lesson_id]

    subjects_str = ", ".join(lesson["subjects"])

    cancel_id = create_request_id()

    await state.update_data(cancel_lesson_id=lesson_id, cancel_id=cancel_id)

    student_confirmation = (

        f"⚠️ ПОДТВЕРЖДЕНИЕ ОТМЕНЫ ЗАНЯТИЯ\n\n"

        f"📚 {subjects_str}\n"

        f"📅 {lesson['date_str']}\n"

        f"🕐 {lesson['time']}\n\n"

        f"Ты уверен(а), что хочешь отменить это занятие?"

    )

    await callback.message.edit_text(

        student_confirmation,

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="✅ Да, отменить", callback_data=f"confirm_student_cancel_{cancel_id}")],

            [InlineKeyboardButton(text="❌ Нет, вернуться в меню", callback_data="back_to_menu")]

        ])

    )

async def confirm_student_cancel_handler(callback: types.CallbackQuery, state: FSMContext):

    """Ученик подтвердил отмену - отправляем репетитору на одобрение"""

    cancel_id = callback.data.replace("confirm_student_cancel_", "")

    data = await state.get_data()

    lesson_id = data.get("cancel_lesson_id")

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id not in confirmed:

        await callback.answer("❌ Занятие не найдено", show_alert=True)

        return

    lesson = confirmed[lesson_id]

    subjects_str = ", ".join(lesson["subjects"])

    student_message = (

        f"⏳ Запрос на отмену отправлен репетитору.\n\n"

        f"Ожидаем его подтверждения..."

    )

    await callback.message.edit_text(student_message, reply_markup=None)

    tutor_cancel_message = (

        f"❌ ЗАПРОС НА ОТМЕНУ ЗАНЯТИЯ\n\n"

        f"👤 Ученик: {lesson['student_name']}\n"

        f"📚 {subjects_str}\n"

        f"📅 {lesson['date_str']}\n"

        f"🕐 {lesson['time']}\n\n"

        f"Согласен(а) ли отменить это занятие?"

    )

    try:

        await callback.bot.send_message(

            TUTOR_ID,

            tutor_cancel_message,

            reply_markup=tutor_cancel_confirm_keyboard(cancel_id)

        )

    except Exception as e:

        print(f"Ошибка при отправке запроса отмены репетитору: {e}")

    pending_cancels = load_json(DATA_DIR / "pending_cancels.json")

    pending_cancels[cancel_id] = {

        "lesson_id": lesson_id,

        "student_id": lesson["student_id"],

        "student_name": lesson["student_name"],

        "subjects": lesson["subjects"],

        "date_str": lesson["date_str"],

        "time": lesson["time"],

        "day": lesson["day"],

        "status": "pending",

        "timestamp": datetime.now().isoformat()

    }

    save_json(DATA_DIR / "pending_cancels.json", pending_cancels)

    await callback.answer()

    await state.clear()

async def tutor_confirm_cancel_handler(callback: types.CallbackQuery):

    """Репетитор подтвердил отмену занятия"""

    cancel_id = callback.data.replace("confirm_cancel_tutor_", "")

    pending_cancels = load_json(DATA_DIR / "pending_cancels.json")

    if cancel_id not in pending_cancels:

        await callback.answer("❌ Запрос на отмену не найден", show_alert=True)

        return

    cancel = pending_cancels[cancel_id]

    lesson_id = cancel["lesson_id"]

    confirmed = load_json(CONFIRMED_FILE)

    if lesson_id in confirmed:

        del confirmed[lesson_id]

        save_json(CONFIRMED_FILE, confirmed)

    schedule = load_json(SCHEDULE_FILE) or DEFAULT_SCHEDULE

    day = cancel["day"]

    time_slot = cancel["time"]

    if time_slot not in schedule[day]:

        schedule[day].append(time_slot)

        schedule[day].sort()

        save_json(SCHEDULE_FILE, schedule)

    del pending_cancels[cancel_id]

    save_json(DATA_DIR / "pending_cancels.json", pending_cancels)

    subjects_str = ", ".join(cancel["subjects"])

    student_message = (

        f"✅ Занятие отменено!\n\n"

        f"📚 {subjects_str}\n"

        f"📅 {cancel['date_str']}\n"

        f"🕐 {cancel['time']}\n\n"

        f"Ты всегда можешь записаться на новое время!"

    )

    await callback.bot.send_message(cancel["student_id"], student_message)

    await callback.message.edit_text(

        callback.message.text + "\n\n✅ *Отмена подтвережена и время освобождено*",

        parse_mode="Markdown"

    )

    await callback.answer("✅ Отмена подтвережена!")

async def tutor_reject_cancel_handler(callback: types.CallbackQuery):

    """Репетитор отклонил отмену занятия"""

    cancel_id = callback.data.replace("reject_cancel_", "")

    pending_cancels = load_json(DATA_DIR / "pending_cancels.json")

    if cancel_id not in pending_cancels:

        await callback.answer("❌ Запрос на отмену не найден", show_alert=True)

        return

    cancel = pending_cancels[cancel_id]

    del pending_cancels[cancel_id]

    save_json(DATA_DIR / "pending_cancels.json", pending_cancels)

    student_message = (

        f"❌ Репетитор отклонил отмену занятия.\n\n"

        f"📚 {', '.join(cancel['subjects'])}\n"

        f"📅 {cancel['date_str']}\n"

        f"🕐 {cancel['time']}\n\n"

        f"Занятие остается в вашем расписании."

    )

    await callback.bot.send_message(cancel["student_id"], student_message)

    await callback.message.edit_text(

        callback.message.text + "\n\n❌ *Отмена отклонена*",

        parse_mode="Markdown"

    )

    await callback.answer("❌ Отмена отклонена")

# ============ УПРАВЛЕНИЕ РАСПИСАНИЕМ РЕПЕТИТОРА ============

async def tutor_confirm_handler(callback: types.CallbackQuery):

    """Репетитор подтвердил занятие"""

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

    except Exception as e:

        print(f"Ошибка при отправке подтверждения ученику: {e}")

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

    """Репетитор отклонил занятие"""

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

    except Exception as e:

        print(f"Ошибка при отправке отклонения ученику: {e}")

    request["status"] = "rejected"

    pending[request_id] = request

    save_json(PENDING_FILE, pending)

    await callback.message.edit_text(

        callback.message.text + "\n\n❌ *Занятие отклонено*",

        parse_mode="Markdown"

    )

    await callback.answer("❌ Запрос отклонен")

async def edit_schedule_button_handler(callback: types.CallbackQuery, state: FSMContext):

    """Кнопка 'Изменить расписание' — доступна только репетитору"""

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

async def set_schedule_handler(message: types.Message, state: FSMContext):

    """Команда /schedule для установки расписания на неделю"""

    help_text = (

        "📝 *Отправь расписание в формате JSON*\n\n"

        "Пример:\n"

        "```json\n"

        "{\n"

        ' "Monday": ["18:00", "19:00", "20:00"],\n'

        ' "Tuesday": ["19:30", "20:30"],\n'

        ' "Wednesday": [],\n'

        ' "Thursday": ["18:15", "19:15"],\n'

        ' "Friday": [],\n'

        ' "Saturday": ["16:30", "17:30"]\n'

        "}\n"

        "```"

    )

    await message.answer(help_text, parse_mode="Markdown")

    await state.set_state(TutorScheduleStates.waiting_for_schedule_json)

async def schedule_json_handler(message: types.Message, state: FSMContext):

    """Получить расписание в JSON формате"""

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

# ============ ПРОВЕРКА И ОТПРАВКА НАПОМИНАНИЙ ============

async def send_reminders(bot: Bot):

    """Проверять и отправлять напоминания за час до занятия"""

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

                        print(f"✅ Напоминание отправлено ученику {lesson['student_name']}")

                    except Exception as e:

                        print(f"❌ Ошибка при отправке напоминания ученику: {e}")

                    try:

                        await bot.send_message(TUTOR_ID, tutor_reminder)

                        print(f"✅ Напоминание отправлено репетитору")

                    except Exception as e:

                        print(f"❌ Ошибка при отправке напоминания репетитору: {e}")

                    lesson["reminder_sent"] = True

                    confirmed[lesson_id] = lesson

                    save_json(CONFIRMED_FILE, confirmed)

                elif time_until_lesson < timedelta(0):

                    del confirmed[lesson_id]

                    save_json(CONFIRMED_FILE, confirmed)

            await asyncio.sleep(30)

        except Exception as e:

            print(f"❌ Ошибка в функции отправки напоминаний: {e}")

            await asyncio.sleep(30)

# ============================================================================

# MAIN

# ============================================================================

async def main():

    bot = Bot(token=TOKEN)

    storage = MemoryStorage()

    dp = Dispatcher(storage=storage)

    # ВАЖНО: Регистрируем обработчики в ПРАВИЛЬНОМ порядке!

    # 1. Команды

    dp.message.register(start_handler, Command("start"))

    dp.message.register(set_schedule_handler, Command("schedule"))

    # 2. Обработчик меню - ПЕРЕД состояниями!

    dp.message.register(menu_button_handler, F.text == "☰ Меню")

    # 3. Обработчики состояний

    dp.message.register(first_lesson_name_handler, FirstLessonStates.waiting_for_name)

    dp.message.register(first_lesson_class_handler, FirstLessonStates.waiting_for_class)

    dp.message.register(schedule_json_handler, TutorScheduleStates.waiting_for_schedule_json)

    # 4. Обработчики callback'ов

    dp.callback_query.register(first_lesson_handler, F.data == "first_lesson")

    dp.callback_query.register(repeat_lesson_handler, F.data == "repeat_lesson")

    dp.callback_query.register(reschedule_lesson_handler, F.data == "reschedule_lesson")

    dp.callback_query.register(cancel_lesson_handler, F.data == "cancel_lesson")

    dp.callback_query.register(edit_schedule_button_handler, F.data == "edit_schedule")

    dp.callback_query.register(back_to_menu_handler, F.data == "back_to_menu")

    dp.callback_query.register(subject_select_handler, F.data.startswith("subject_"))

    dp.callback_query.register(subjects_done_handler, F.data == "subjects_done")

    dp.callback_query.register(time_select_handler, F.data.startswith("time_"))

    dp.callback_query.register(select_reschedule_handler, F.data.startswith("select_reschedule_"))

    dp.callback_query.register(select_new_time_handler, F.data.startswith("newtime_"))

    dp.callback_query.register(tutor_confirm_reschedule_handler, F.data.startswith("confirm_reschedule_"))

    dp.callback_query.register(tutor_reject_reschedule_handler, F.data.startswith("reject_reschedule_"))

    dp.callback_query.register(select_cancel_handler, F.data.startswith("select_cancel_"))

    dp.callback_query.register(confirm_student_cancel_handler, F.data.startswith("confirm_student_cancel_"))

    dp.callback_query.register(tutor_confirm_cancel_handler, F.data.startswith("confirm_cancel_tutor_"))

    dp.callback_query.register(tutor_reject_cancel_handler, F.data.startswith("reject_cancel_"))

    dp.callback_query.register(tutor_confirm_handler, F.data.startswith("confirm_") & ~F.data.startswith("confirm_cancel_") & ~F.data.startswith("confirm_reschedule_") & ~F.data.startswith("confirm_student_cancel_"))

    dp.callback_query.register(tutor_reject_handler, F.data.startswith("reject_") & ~F.data.startswith("reject_reschedule_") & ~F.data.startswith("reject_cancel_"))

    reminder_task = asyncio.create_task(send_reminders(bot))

    print("🤖 Бот запущен! Ожидаем сообщений...")

    try:

        await dp.start_polling(bot)

    finally:

        reminder_task.cancel()

        await bot.session.close()

if __name__ == "__main__":

    asyncio.run(main())
