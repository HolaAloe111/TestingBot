import asyncio
import logging
import random
import os
import time
import config  # Assuming this file contains BOT_TOKEN
from openpyxl import Workbook, load_workbook
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    FSInputFile,
)
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

# --- Globals & Constants ---
user_data_storage = (
    {}
)  # In-memory cache for the last active profile of a given Telegram ID during the current session.

# Bot and Dispatcher setup
bot = Bot(
    config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

EXCEL_FILENAME = "persistent_user_data.xlsx"
# Excel Column Mapping (0-indexed for code, 1-indexed for user display)
# A (0): Telegram ID, B (1): Unique ID, C (2): Name, D (3): Age
# E (4): Corsi Max Len, F (5): Corsi Avg Time, G (6): Corsi Detail

IKB = InlineKeyboardButton  # Alias for brevity

# --- Action Selection Keyboards ---
ACTION_SELECTION_KEYBOARD_NEW = InlineKeyboardMarkup(
    inline_keyboard=[
        [IKB(text="Пройти батарею тестов", callback_data="run_test_battery")],
        [
            IKB(
                text="Выбрать отдельный тест",
                callback_data="select_specific_test",
            )
        ],
    ]
)

ACTION_SELECTION_KEYBOARD_RETURNING = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            IKB(
                text="Пройти батарею тестов заново",
                callback_data="run_test_battery",
            )
        ],
        [
            IKB(
                text="Выбрать отдельный тест заново",
                callback_data="select_specific_test",
            )
        ],
    ]
)


# --- FSM States ---
class UserData(StatesGroup):
    waiting_for_first_time_response = State()
    waiting_for_name = State()
    waiting_for_age = State()
    waiting_for_unique_id = State()


class CorsiTestStates(StatesGroup):
    showing_sequence = State()
    waiting_for_user_sequence = State()
    test_completed = State()
    waiting_for_overwrite_confirmation_corsi = State()


# --- Helper Functions ---
def initialize_excel_file():
    if not os.path.exists(EXCEL_FILENAME):
        wb = Workbook()
        ws = wb.active
        headers = [
            "Telegram ID",
            "Unique ID",
            "Name",
            "Age",
            "Corsi - Max Correct Sequence Length",
            "Corsi - Avg Time Per Element (s)",
            "Corsi - Sequence Times Detail",
        ]
        ws.append(headers)
        wb.save(EXCEL_FILENAME)
        logger.info(f"'{EXCEL_FILENAME}' created with headers.")
    else:
        logger.info(f"'{EXCEL_FILENAME}' already exists.")


async def cleanup_corsi_messages(
    state: FSMContext, bot_instance: Bot, final_grid_text: str = None
):
    data = await state.get_data()
    chat_id = data.get('corsi_chat_id')
    if not chat_id:
        logger.warning("No 'corsi_chat_id' in FSM for cleanup.")
        return

    msg_ids_to_process = {
        'delete': [
            data.get('corsi_status_message_id'),
            data.get('corsi_feedback_message_id'),
        ],
        'edit_grid': data.get('corsi_grid_message_id'),
        'edit_confirm': (
            data.get('original_message_id')
            if await state.get_state()
            == CorsiTestStates.waiting_for_overwrite_confirmation_corsi.state
            else None
        ),
    }

    for msg_id in msg_ids_to_process['delete']:
        if msg_id:
            try:
                await bot_instance.delete_message(
                    chat_id=chat_id, message_id=msg_id
                )
            except TelegramBadRequest as e:
                logger.warning(
                    f"Could not delete msg {msg_id} in {chat_id}: {e}"
                )

    if msg_ids_to_process['edit_grid']:
        try:
            text = (
                final_grid_text
                if final_grid_text
                else "Тест Корси завершен или отменен."
            )
            await bot_instance.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=msg_ids_to_process['edit_grid'],
                reply_markup=None,
            )
        except TelegramBadRequest as e:
            logger.warning(
                f"Could not edit grid msg {msg_ids_to_process['edit_grid']} in {chat_id}: {e}"
            )

    if msg_ids_to_process['edit_confirm']:
        try:
            text = final_grid_text if final_grid_text else "Действие отменено."
            await bot_instance.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=msg_ids_to_process['edit_confirm'],
                reply_markup=None,
            )
        except TelegramBadRequest as e:
            logger.warning(
                f"Could not edit confirm msg {msg_ids_to_process['edit_confirm']} in {chat_id}: {e}"
            )


async def send_main_action_menu(
    message_or_callback: [Message, CallbackQuery],
    keyboard_markup: InlineKeyboardMarkup,
    text: str = "Выберите дальнейшее действие:",
):
    chat_id = (
        message_or_callback.chat.id
        if isinstance(message_or_callback, Message)
        else message_or_callback.message.chat.id
    )
    try:
        await bot.send_message(chat_id, text, reply_markup=keyboard_markup)
    except Exception as e:
        logger.error(f"Error in send_main_action_menu for chat {chat_id}: {e}")


# --- Corsi Test Logic ---
async def show_corsi_sequence(trigger_message: Message, state: FSMContext):
    data = await state.get_data()
    current_sequence_length = data.get('current_sequence_length', 2)
    corsi_chat_id = data.get('corsi_chat_id', trigger_message.chat.id)
    grid_message_id_from_state = data.get('corsi_grid_message_id')

    restart_button_row = [
        IKB(text="🔄", callback_data="corsi_restart_current_test")
    ]
    button_indices = list(range(9))
    random.shuffle(button_indices)
    correct_sequence = button_indices[:current_sequence_length]
    await state.update_data(
        correct_sequence=correct_sequence, user_input_sequence=[]
    )

    base_buttons = [
        IKB(text="🟣", callback_data=f"corsi_button_{i}") for i in range(9)
    ]
    base_keyboard_grid_rows = [base_buttons[i : i + 3] for i in range(0, 9, 3)]
    keyboard_for_base_markup_with_restart = [
        row[:] for row in base_keyboard_grid_rows
    ]
    keyboard_for_base_markup_with_restart.append(restart_button_row)
    base_markup_with_restart = InlineKeyboardMarkup(
        inline_keyboard=keyboard_for_base_markup_with_restart
    )

    if grid_message_id_from_state:
        try:
            await bot.edit_message_text(
                chat_id=corsi_chat_id,
                message_id=grid_message_id_from_state,
                text="Тест Корси",
                reply_markup=base_markup_with_restart,
            )
        except TelegramBadRequest:
            grid_msg_obj = await bot.send_message(
                corsi_chat_id,
                "Тест Корси",
                reply_markup=base_markup_with_restart,
            )
            grid_message_id_from_state = grid_msg_obj.message_id
    else:
        grid_msg_obj = await bot.send_message(
            corsi_chat_id, "Тест Корси", reply_markup=base_markup_with_restart
        )
        grid_message_id_from_state = grid_msg_obj.message_id
    await state.update_data(
        corsi_grid_message_id=grid_message_id_from_state,
        corsi_chat_id=corsi_chat_id,
    )

    corsi_status_message_id = data.get('corsi_status_message_id')
    status_text_queue = (
        ["Приготовитесь..."]
        + [f"{i}..." for i in range(3, 0, -1)]
        + ["Запоминайте..."]
    )
    for i, text in enumerate(status_text_queue):
        if not corsi_status_message_id:
            status_obj = await bot.send_message(corsi_chat_id, text)
            corsi_status_message_id = status_obj.message_id
            await state.update_data(
                corsi_status_message_id=corsi_status_message_id
            )
        else:
            try:
                await bot.edit_message_text(
                    text=text,
                    chat_id=corsi_chat_id,
                    message_id=corsi_status_message_id,
                )
            except TelegramBadRequest:
                status_obj = await bot.send_message(corsi_chat_id, text)
                corsi_status_message_id = status_obj.message_id
                await state.update_data(
                    corsi_status_message_id=corsi_status_message_id
                )
        if i < len(status_text_queue) - 1:
            await asyncio.sleep(1)
        else:
            await asyncio.sleep(0.5)

    for button_index in correct_sequence:
        flashed_rows = [
            [
                IKB(
                    text="🟡" if r * 3 + c == button_index else "🟣",
                    callback_data=f"corsi_button_{r * 3 + c}",
                )
                for c in range(3)
            ]
            for r in range(3)
        ]
        flashed_rows.append(list(restart_button_row))
        flashed_markup = InlineKeyboardMarkup(inline_keyboard=flashed_rows)
        try:
            await bot.edit_message_reply_markup(
                chat_id=corsi_chat_id,
                message_id=grid_message_id_from_state,
                reply_markup=flashed_markup,
            )
            await asyncio.sleep(0.5)
            await bot.edit_message_reply_markup(
                chat_id=corsi_chat_id,
                message_id=grid_message_id_from_state,
                reply_markup=base_markup_with_restart,
            )
            await asyncio.sleep(0.2)
        except TelegramBadRequest:
            return

    try:
        await bot.edit_message_text(
            text="Повторите последовательность:",
            chat_id=corsi_chat_id,
            message_id=corsi_status_message_id,
        )
    except TelegramBadRequest:
        return

    await state.update_data(sequence_start_time=time.time())
    await state.set_state(CorsiTestStates.waiting_for_user_sequence)


@dp.callback_query(
    F.data.startswith("corsi_button_"),
    CorsiTestStates.waiting_for_user_sequence,
)
async def handle_corsi_button_press(
    callback: CallbackQuery, state: FSMContext
):
    await callback.answer()
    button_index = int(callback.data.split("_")[-1])
    data = await state.get_data()
    user_input_sequence = data.get('user_input_sequence', []) + [button_index]

    corsi_grid_message_id = data.get('corsi_grid_message_id')
    corsi_chat_id = data.get('corsi_chat_id')
    if not corsi_grid_message_id or not corsi_chat_id:
        logger.error("Corsi grid/chat ID missing.")
        await callback.message.answer("Ошибка. /start")
        await state.clear()
        return

    new_rows = [
        [
            IKB(
                text="🟡" if r * 3 + c in user_input_sequence else "🟣",
                callback_data=f"corsi_button_{r * 3 + c}",
            )
            for c in range(3)
        ]
        for r in range(3)
    ]
    new_rows.append(
        [IKB(text="🔄", callback_data="corsi_restart_current_test")]
    )
    try:
        await bot.edit_message_reply_markup(
            chat_id=corsi_chat_id,
            message_id=corsi_grid_message_id,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=new_rows),
        )
    except TelegramBadRequest as e:
        logger.error(f"Error editing markup on button press: {e}")

    await state.update_data(user_input_sequence=user_input_sequence)
    if len(user_input_sequence) == len(data.get('correct_sequence', [])):
        await evaluate_user_sequence(callback.message, state)


@dp.callback_query(
    F.data == "corsi_restart_current_test", StateFilter(CorsiTestStates)
)
async def on_corsi_restart_current_test(
    callback: CallbackQuery, state: FSMContext
):
    await callback.answer(text='Тест Корси перезапущен.', show_alert=True)
    await cleanup_corsi_messages(
        state, bot, final_grid_text="Тест был перезапущен пользователем."
    )
    await state.clear()
    await bot.send_message(
        callback.message.chat.id,
        "Тест был перезапущен. Вы можете начать новый тест из меню (которое появляется после /start).",
    )


async def save_corsi_results(message_context: Message, state: FSMContext):
    data = await state.get_data()
    current_interactor_telegram_id = message_context.chat.id

    unique_id = data.get(
        'unique_id'
    )  # This is the unique_id of the active profile for the test
    profile_telegram_id = data.get('profile_telegram_id')
    profile_name = data.get('profile_name')
    profile_age = data.get('profile_age')

    if not unique_id:
        logger.error(
            f"CRITICAL: 'unique_id' for test session not found in FSM state for interactor {current_interactor_telegram_id}. Data: {data}"
        )
        summary_text_error = "Тест Корси завершен, но произошла критическая ошибка при сохранении результатов (не найден ID профиля сессии). Свяжитесь с администратором."
        await message_context.answer(summary_text_error)
        await cleanup_corsi_messages(
            state, bot, final_grid_text="Тест завершен с ошибкой ID профиля."
        )
        await state.clear()
        return

    if not profile_telegram_id:  # Should be set by _proceed_to_corsi_test
        logger.warning(
            f"Profile's original telegram_id ('profile_telegram_id') not found in FSM for UID {unique_id}. Using current interactor's TG ID {current_interactor_telegram_id} as fallback for Excel row if new."
        )
        profile_telegram_id = current_interactor_telegram_id

    sequence_times = data.get('sequence_times', [])
    corsi_max_len = (
        max(item['len'] for item in sequence_times) if sequence_times else 0
    )
    corsi_avg_time_per_element = 0.0
    if sequence_times:
        valid_sequences = [item for item in sequence_times if item['len'] > 0]
        if valid_sequences:
            total_avg_time_sum = sum(
                item['time'] / item['len'] for item in valid_sequences
            )
            corsi_avg_time_per_element = total_avg_time_sum / len(
                valid_sequences
            )
    corsi_detail_string = "; ".join(
        [f"L{item['len']}:{item['time']:.2f}s" for item in sequence_times]
    )

    # Update in-memory cache for the current interactor, reflecting the profile they tested under
    user_data_storage[current_interactor_telegram_id] = {
        'unique_id': unique_id,
        'name': profile_name,  # Name from the active profile used in test
        'age': profile_age,  # Age from the active profile used in test
        'telegram_id': profile_telegram_id,  # Canonical TG ID of the profile
        'corsi_max_len': corsi_max_len,
        'corsi_avg_time': round(corsi_avg_time_per_element, 2),
        'corsi_detail': corsi_detail_string,
    }

    try:  # Excel update
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        updated_row_in_excel = False
        for row_idx, row_cells_tuple in enumerate(
            ws.iter_rows(min_row=2), start=2
        ):
            if (
                len(row_cells_tuple) > 1
                and row_cells_tuple[1].value == unique_id
            ):
                if row_cells_tuple[0].value != profile_telegram_id:
                    logger.warning(
                        f"Excel TG ID {row_cells_tuple[0].value} differs from profile's canonical TG ID {profile_telegram_id} for UID {unique_id}."
                    )
                # Always update based on unique_id
                ws.cell(row=row_idx, column=5).value = corsi_max_len
                ws.cell(row=row_idx, column=6).value = round(
                    corsi_avg_time_per_element, 2
                )
                ws.cell(row=row_idx, column=7).value = corsi_detail_string
                updated_row_in_excel = True
                break

        if not updated_row_in_excel:
            logger.warning(
                f"UID {unique_id} (Profile TG ID: {profile_telegram_id}) not found in Excel. Appending with profile data from FSM."
            )
            ws.append(
                [
                    profile_telegram_id,
                    unique_id,
                    profile_name if profile_name else 'N/A_FSM',
                    profile_age if profile_age else 'N/A_FSM',
                    corsi_max_len,
                    round(corsi_avg_time_per_element, 2),
                    corsi_detail_string,
                ]
            )
        wb.save(EXCEL_FILENAME)
        logger.info(
            f"Corsi results saved to Excel for UID {unique_id} (Profile TG ID: {profile_telegram_id})."
        )
    except Exception as e:
        logger.error(f"Error saving Corsi results to Excel: {e}")

    summary_text = (
        f"Тест Корси завершен!\nМаксимальная длина последовательности: {corsi_max_len}\n"
        f"Среднее время на элемент: {round(corsi_avg_time_per_element, 2)} сек\nДетализация: {corsi_detail_string}"
    )
    await message_context.answer(summary_text)
    await cleanup_corsi_messages(
        state, bot, final_grid_text="Тест Корси завершен."
    )
    await state.clear()
    logger.info(
        f"Corsi test FSM cleared for user {current_interactor_telegram_id}."
    )


async def evaluate_user_sequence(message_context: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('corsi_chat_id', message_context.chat.id)
    user_seq = data.get('user_input_sequence', [])
    correct_seq = data.get('correct_sequence', [])
    curr_len = data.get('current_sequence_length', 2)
    err_count = data.get('error_count', 0)
    seq_times = data.get('sequence_times', [])
    start_time = data.get('sequence_start_time', 0)
    fb_id = data.get('corsi_feedback_message_id')
    time_taken = time.time() - start_time

    if user_seq == correct_seq:
        seq_times.append({'len': curr_len, 'time': time_taken})
        curr_len += 1
        err_count = 0
        txt_b, txt_n = "<b>Верно!</b>", "Верно!"
        if not fb_id:
            fb_id = (
                await bot.send_message(
                    chat_id, txt_b, parse_mode=ParseMode.HTML
                )
            ).message_id
            await state.update_data(corsi_feedback_message_id=fb_id)
        else:
            try:
                await bot.edit_message_text(
                    txt_b,
                    chat_id=chat_id,
                    message_id=fb_id,
                    parse_mode=ParseMode.HTML,
                )
            except TelegramBadRequest as e:
                logger.warning(f"Err bold: {e}")
        await asyncio.sleep(0.5)
        if fb_id:
            try:
                await bot.edit_message_text(
                    txt_n, chat_id=chat_id, message_id=fb_id, parse_mode=None
                )
            except TelegramBadRequest as e:
                (logger.warning(f"Err norm: {e}"))

        if curr_len > 9:
            await save_corsi_results(message_context, state)
        else:
            await state.update_data(
                current_sequence_length=curr_len,
                error_count=err_count,
                sequence_times=seq_times,
                user_input_sequence=[],
            )
            await show_corsi_sequence(message_context, state)
    else:
        err_count += 1
        txt_err = "<b>Ошибка! Попробуйте ещё раз</b>"
        if not fb_id:
            fb_id = (
                await bot.send_message(
                    chat_id, txt_err, parse_mode=ParseMode.HTML
                )
            ).message_id
            await state.update_data(corsi_feedback_message_id=fb_id)
        else:
            try:
                await bot.edit_message_text(
                    txt_err,
                    chat_id=chat_id,
                    message_id=fb_id,
                    parse_mode=ParseMode.HTML,
                )
            except TelegramBadRequest as e:
                logger.warning(f"Err err_fb: {e}")
        if err_count >= 2:
            await save_corsi_results(message_context, state)
        else:
            await state.update_data(
                error_count=err_count, user_input_sequence=[]
            )
            await show_corsi_sequence(message_context, state)


# --- Registration and Main Menu Handlers ---
@dp.message(Command("start"))
async def start_command_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(UserData.waiting_for_first_time_response)
    first_time_kbd = InlineKeyboardMarkup(
        inline_keyboard=[
            [IKB(text="Да", callback_data="user_is_new")],
            [IKB(text="Нет", callback_data="user_is_returning")],
        ]
    )
    await message.answer(
        "Вы впервые пользуетесь ботом?", reply_markup=first_time_kbd
    )


@dp.callback_query(
    F.data == "user_is_new", UserData.waiting_for_first_time_response
)
async def handle_user_is_new_callback(
    callback: CallbackQuery, state: FSMContext
):
    await callback.answer()
    try:
        await callback.message.edit_reply_markup()
    except TelegramBadRequest:
        pass
    await state.set_state(UserData.waiting_for_name)
    await callback.message.answer('Привет! Давайте начнем. Как вас зовут?')


@dp.callback_query(
    F.data == "user_is_returning", UserData.waiting_for_first_time_response
)
async def handle_user_is_returning_callback(
    callback: CallbackQuery, state: FSMContext
):
    await callback.answer()
    try:
        await callback.message.edit_reply_markup()
    except TelegramBadRequest:
        pass
    await state.set_state(UserData.waiting_for_unique_id)
    await callback.message.answer("Введите ваш уникальный идентификатор")


@dp.message(UserData.waiting_for_unique_id)
async def process_unique_id_input(message: Message, state: FSMContext):
    try:
        entered_unique_id = int(message.text)
    except ValueError:
        await message.answer(
            "Пожалуйста, введите корректный числовой идентификатор."
        )
        return
    try:
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        user_found = False
        for row_values in ws.iter_rows(min_row=2, values_only=True):
            if (
                row_values
                and len(row_values) > 6
                and row_values[1] == entered_unique_id
            ):
                tg_excel, name_excel, age_excel = (
                    row_values[0],
                    str(row_values[2]),
                    str(row_values[3]),
                )
                c_max, c_avg, c_det = (
                    row_values[4],
                    row_values[5],
                    row_values[6],
                )
                active_profile = {
                    'active_unique_id': entered_unique_id,
                    'active_name': name_excel,
                    'active_age': age_excel,
                    'active_telegram_id': tg_excel,
                    'corsi_max_len': c_max,
                    'corsi_avg_time': float(c_avg) if c_avg else None,
                    'corsi_detail': c_det,
                }
                user_data_storage[message.from_user.id] = active_profile
                await state.set_data(active_profile)
                await state.set_state(None)
                await message.answer(
                    f"Авторизация прошла успешно, {name_excel}!"
                )
                await send_main_action_menu(
                    message, ACTION_SELECTION_KEYBOARD_RETURNING
                )
                user_found = True
                break
        if not user_found:
            kbd = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        IKB(
                            text="Попробовать снова",
                            callback_data="try_id_again",
                        )
                    ],
                    [
                        IKB(
                            text="Зарегистрироваться как новый",
                            callback_data="register_new_after_fail",
                        )
                    ],
                ]
            )
            await message.answer(
                "Уникальный идентификатор не найден.", reply_markup=kbd
            )
    except Exception as e:
        logger.error(f"ID check error: {e}")
        await message.answer("Ошибка ID.")


@dp.callback_query(F.data == "try_id_again", UserData.waiting_for_unique_id)
async def handle_try_id_again_callback(
    callback: CallbackQuery, state: FSMContext
):
    await callback.answer()
    try:
        await callback.message.edit_reply_markup()
    except TelegramBadRequest:
        pass
    await callback.message.answer("Введите ваш уникальный идентификатор")


@dp.callback_query(
    F.data == "register_new_after_fail", UserData.waiting_for_unique_id
)
async def handle_register_new_after_fail_callback(
    callback: CallbackQuery, state: FSMContext
):
    await callback.answer()
    try:
        await callback.message.edit_reply_markup()
    except TelegramBadRequest:
        pass
    await state.set_state(UserData.waiting_for_name)
    await callback.message.answer('Как вас зовут?')


@dp.message(UserData.waiting_for_name)
async def process_name_input(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(UserData.waiting_for_age)
    await message.answer('Отлично! Теперь введите ваш возраст.')


@dp.message(UserData.waiting_for_age)
async def process_age_input(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (0 < int(message.text) < 120):
        await message.answer("Введите возраст цифрами.")
        return
    fsm_data = await state.get_data()
    name, age, tg_id = fsm_data.get('name'), message.text, message.from_user.id
    new_uid = None
    try:
        wb_check = load_workbook(EXCEL_FILENAME)
        ws_check = wb_check.active
        existing_ids = {
            r[1]
            for r in ws_check.iter_rows(min_row=2, values_only=True)
            if r and len(r) > 1 and r[1]
        }
        new_uid = random.randint(1000000, 9999999)
        while new_uid in existing_ids:
            new_uid = random.randint(1000000, 9999999)
    except Exception as e:
        logger.error(f"New ID gen error: {e}")
        await message.answer("Ошибка ID.")
        await state.clear()
        return

    active_profile = {
        'active_telegram_id': tg_id,
        'active_unique_id': new_uid,
        'active_name': name,
        'active_age': age,
        'corsi_max_len': None,
        'corsi_avg_time': None,
        'corsi_detail': None,
    }
    user_data_storage[tg_id] = active_profile

    try:
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        ws.append([tg_id, new_uid, name, age, None, None, None])
        wb.save(EXCEL_FILENAME)
        logger.info(f"New user {tg_id} (UID:{new_uid}) registered.")
    except Exception as e:
        logger.error(f"Save new user error: {e}")
        await message.answer("Ошибка сохранения.")
        if tg_id in user_data_storage:
            del user_data_storage[tg_id]
        await state.clear()
        return

    await message.answer(
        f"Спасибо, {name}! Ваши данные сохранены. Ваш UID: {new_uid}."
    )
    await state.set_data(active_profile)
    await state.set_state(None)
    await send_main_action_menu(message, ACTION_SELECTION_KEYBOARD_NEW)


# --- Test Initiation and Overwrite Confirmation ---
async def _proceed_to_corsi_test(message_context: Message, state: FSMContext):
    try:
        await message_context.edit_text(
            "Подготовка к тесту Корси...", reply_markup=None
        )
    except TelegramBadRequest as e:
        logger.info(f"Edit failed pre-Corsi: {e}")
        await message_context.answer("Подготовка к тесту Корси...")

    fsm_data = await state.get_data()
    active_unique_id = fsm_data.get('active_unique_id')
    active_name = fsm_data.get('active_name')
    active_age = fsm_data.get('active_age')
    active_telegram_id = fsm_data.get('active_telegram_id')

    if not active_unique_id:
        logger.error(
            f"CRITICAL: No active_unique_id in FSM to start Corsi test for user {message_context.from_user.id}. FSM: {fsm_data}"
        )
        await message_context.answer(
            "Критическая ошибка: не удалось определить ваш профиль для теста. Пожалуйста, начните заново с /start."
        )
        await state.clear()
        return

    await state.set_state(CorsiTestStates.showing_sequence)
    # Store all necessary active profile data for the test session under non-"active_" prefixed keys
    # These will be used by save_corsi_results
    await state.update_data(
        unique_id=active_unique_id,
        profile_name=active_name,
        profile_age=active_age,
        profile_telegram_id=active_telegram_id,
        current_sequence_length=2,
        error_count=0,
        sequence_times=[],
        correct_sequence=[],
        user_input_sequence=[],
        sequence_start_time=0,
        corsi_grid_message_id=None,
        corsi_status_message_id=None,
        corsi_chat_id=message_context.chat.id,
        corsi_feedback_message_id=None,
    )
    await show_corsi_sequence(message_context, state)


async def check_corsi_data_and_proceed(
    trigger_event: [CallbackQuery, Message], state: FSMContext
):
    user_id = trigger_event.from_user.id
    msg_ctx = (
        trigger_event.message
        if isinstance(trigger_event, CallbackQuery)
        else trigger_event
    )
    fsm_data = await state.get_data()

    active_unique_id = fsm_data.get('active_unique_id')

    if not active_unique_id:
        logger.error(
            f"User {user_id} has no 'active_unique_id' in FSM. Cannot check/proceed. FSM: {fsm_data}"
        )
        await msg_ctx.answer(
            "Ошибка: ваш профиль не активен. Пожалуйста, пройдите /start."
        )
        return

    # Excel lookup uses this active_unique_id
    corsi_data_exists_in_excel = False
    try:
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        for r_vals in ws.iter_rows(min_row=2, values_only=True):
            if (
                r_vals
                and len(r_vals) > 6
                and r_vals[1] == active_unique_id
                and any(r_vals[i] for i in [4, 5, 6])
            ):
                corsi_data_exists_in_excel = True
                break
    except Exception as e:
        logger.error(f"Excel check error for UID {active_unique_id}: {e}")

    if corsi_data_exists_in_excel:
        kbd = InlineKeyboardMarkup(
            inline_keyboard=[
                [IKB(text="Да", callback_data="overwrite_corsi_confirm")],
                [IKB(text="Нет", callback_data="overwrite_corsi_cancel")],
            ]
        )
        await state.set_state(
            CorsiTestStates.waiting_for_overwrite_confirmation_corsi
        )
        # Pass active_unique_id and other active profile data to the confirmation state
        await state.update_data(
            original_message_id=msg_ctx.message_id,
            original_chat_id=msg_ctx.chat.id,
            active_unique_id_to_overwrite=active_unique_id,
            stored_active_name=fsm_data.get('active_name'),
            stored_active_age=fsm_data.get('active_age'),
            stored_active_telegram_id=fsm_data.get('active_telegram_id'),
        )
        try:
            await msg_ctx.edit_text(
                "У вас есть сохраненные результаты Теста Корси. Перезаписать их?",
                reply_markup=kbd,
            )
        except TelegramBadRequest:
            await msg_ctx.answer(
                "У вас есть сохраненные результаты Теста Корси. Перезаписать их?",
                reply_markup=kbd,
            )
    else:
        await _proceed_to_corsi_test(msg_ctx, state)


@dp.callback_query(F.data == "select_specific_test")
async def on_select_specific_test_callback(
    cb: CallbackQuery, state: FSMContext
):
    await cb.answer()
    data = await state.get_data()
    if not data.get('active_unique_id'):
        await cb.message.answer("Сначала /start.")
        try:
            await cb.message.edit_reply_markup()
        except:
            pass
            return
    kbd = InlineKeyboardMarkup(
        inline_keyboard=[
            [IKB(text="Тест Корси", callback_data="initiate_corsi_test")]
        ]
    )
    try:
        await cb.message.edit_text("Выберите тест:", reply_markup=kbd)
    except TelegramBadRequest as e:
        logger.info(f"Edit failed: {e}")
        await cb.message.answer("Выберите тест:", reply_markup=kbd)


@dp.callback_query(F.data == "run_test_battery")
async def on_run_test_battery_callback(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await check_corsi_data_and_proceed(cb, state)


@dp.callback_query(F.data == "initiate_corsi_test")
async def on_initiate_corsi_test_callback(
    cb: CallbackQuery, state: FSMContext
):
    await cb.answer()
    await check_corsi_data_and_proceed(cb, state)


@dp.callback_query(
    F.data == "overwrite_corsi_confirm",
    CorsiTestStates.waiting_for_overwrite_confirmation_corsi,
)
async def handle_overwrite_corsi_confirm_callback(
    cb: CallbackQuery, state: FSMContext
):
    await cb.answer()
    fsm_data = await state.get_data()
    unique_id_to_use = fsm_data.get(
        'active_unique_id_to_overwrite'
    )  # This was the one confirmed for overwrite
    # Restore the full active profile using the stored values from before confirmation
    await state.update_data(
        active_unique_id=unique_id_to_use,
        active_name=fsm_data.get('stored_active_name'),
        active_age=fsm_data.get('stored_active_age'),
        active_telegram_id=fsm_data.get('stored_active_telegram_id'),
    )
    await _proceed_to_corsi_test(cb.message, state)


@dp.callback_query(
    F.data == "overwrite_corsi_cancel",
    CorsiTestStates.waiting_for_overwrite_confirmation_corsi,
)
async def handle_overwrite_corsi_cancel_callback(
    cb: CallbackQuery, state: FSMContext
):
    await cb.answer("Отменено.", show_alert=False)
    try:
        await cb.message.edit_text(
            "Запуск теста Корси отменен.", reply_markup=None
        )
    except TelegramBadRequest as e:
        logger.info(f"Edit on Corsi cancel failed: {e}")

    data = await state.get_data()
    restored_profile_data = {
        'active_unique_id': data.get('active_unique_id_to_overwrite'),
        'active_name': data.get('stored_active_name'),
        'active_age': data.get('stored_active_age'),
        'active_telegram_id': data.get('stored_active_telegram_id'),
    }
    await state.set_data(restored_profile_data)
    await state.set_state(None)

    if restored_profile_data.get('active_unique_id'):
        await send_main_action_menu(
            cb.message,
            ACTION_SELECTION_KEYBOARD_RETURNING,
            text="Выберите действие:",
        )
    else:
        await send_main_action_menu(
            cb.message,
            ACTION_SELECTION_KEYBOARD_NEW,
            text="Выберите действие:",
        )


# --- Utility Handlers ---
@dp.message(Command("mydata"))
async def show_my_data_command(message: Message, state: FSMContext):
    fsm_data = await state.get_data()
    active_unique_id = fsm_data.get('active_unique_id')
    user_id = message.from_user.id

    if not active_unique_id:
        # If no active profile in FSM, check session cache (user_data_storage)
        cached_profile = user_data_storage.get(user_id)
        if cached_profile and cached_profile.get(
            'active_unique_id'
        ):  # Ensure it's a full active profile from cache
            active_unique_id = cached_profile.get('active_unique_id')
            logger.info(
                f"/mydata: No active FSM profile, using cached UID {active_unique_id} for TG ID {user_id}"
            )
            # Populate FSM with this cached active profile for consistency in display
            await state.set_data(
                cached_profile
            )  # This sets the whole cached dict as FSM data
            fsm_data = cached_profile  # Use this for current display
        else:
            await message.answer(
                "Пожалуйста, сначала войдите в свой профиль или зарегистрируйтесь с помощью команды /start."
            )
            return

    # Display data based on active_unique_id from FSM (which might have been populated from cache)
    display_name = fsm_data.get('active_name', 'N/A')
    display_age = fsm_data.get('active_age', 'N/A')
    display_telegram_id = fsm_data.get('active_telegram_id', 'N/A')
    display_corsi_max = fsm_data.get(
        'corsi_max_len', fsm_data.get('active_corsi_max_len', 'N/A')
    )
    display_corsi_avg = fsm_data.get(
        'corsi_avg_time', fsm_data.get('active_corsi_avg_time', 'N/A')
    )
    display_corsi_detail = fsm_data.get(
        'corsi_detail', fsm_data.get('active_corsi_detail', 'N/A')
    )

    # For /mydata, always try to show the most definitive data from Excel for the active_unique_id
    excel_profile_found = False
    try:
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        for row_values in ws.iter_rows(min_row=2, values_only=True):
            if (
                row_values
                and len(row_values) > 6
                and row_values[1] == active_unique_id
            ):
                display_telegram_id, display_name, display_age = (
                    row_values[0],
                    str(row_values[2]),
                    str(row_values[3]),
                )
                display_corsi_max, display_corsi_avg, display_corsi_detail = (
                    row_values[4],
                    row_values[5],
                    row_values[6],
                )
                excel_profile_found = True
                break
        if not excel_profile_found:
            logger.warning(
                f"/mydata: Active UID {active_unique_id} not found in Excel. Displaying FSM/cache data."
            )
    except FileNotFoundError:
        logger.warning(
            f"/mydata: Excel file not found. Displaying FSM/cache data."
        )
    except Exception as e:
        logger.error(
            f"Error loading Excel for /mydata (UID: {active_unique_id}): {e}"
        )

    response_text = (
        f"Данные для активного профиля ID: {active_unique_id}\n"
        f"Имя: {display_name}\nВозраст: {display_age}\n"
        f"Telegram ID (привязанный к профилю): {display_telegram_id}\n"
        f"Corsi Max Length: {display_corsi_max if display_corsi_max is not None else 'N/A'}\n"
        f"Corsi Avg Time: {float(display_corsi_avg) if display_corsi_avg is not None else 'N/A'}\n"
        f"Corsi Detail: {display_corsi_detail if display_corsi_detail is not None else 'N/A'}"
    )
    await message.answer(response_text)


@dp.message(Command("export"))
async def export_data_to_excel_command(message: Message, state: FSMContext):
    if os.path.exists(EXCEL_FILENAME):
        await message.reply_document(
            FSInputFile(EXCEL_FILENAME), caption="Данные пользователей."
        )
    else:
        await message.answer("Файл не найден.")


@dp.message(Command("restart"))
async def command_restart_test_handler(message: Message, state: FSMContext):
    curr_state = await state.get_state()
    if curr_state and any(s.state == curr_state for s in CorsiTestStates):
        await cleanup_corsi_messages(
            state, bot, final_grid_text="Тест остановлен /restart."
        )
    await state.clear()
    await message.answer("Процесс остановлен. /start для начала.")


# --- Main Bot Execution ---
async def main():
    initialize_excel_file()
    # Removed user_data_storage pre-population from Excel at startup.
    # user_data_storage will now be populated on-the-fly during user sessions.
    logger.info(
        "User data storage will be populated during user sessions, not pre-loaded at startup."
    )
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Bot stopped by user.')
    except Exception as e:
        logging.error(f"Unhandled main exception: {e}", exc_info=True)
