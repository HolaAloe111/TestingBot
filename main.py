import asyncio
import logging
import random
import os
import time
import config
from openpyxl import Workbook, load_workbook
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, \
    FSInputFile
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest  # For handling message deletion errors

user_data_storage = {}

bot = Bot(config.BOT_TOKEN,
          default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def initialize_excel_file():
    excel_filename = "persistent_user_data.xlsx"
    if not os.path.exists(excel_filename):
        wb = Workbook()
        ws = wb.active

        ws['A1'] = "Telegram ID"
        ws['B1'] = "Unique ID"
        ws['C1'] = "Name"
        ws['D1'] = "Age"
        ws['E1'] = "Corsi - Max Correct Sequence Length"
        ws['F1'] = "Corsi - Avg Time Per Element (s)"
        ws['G1'] = "Corsi - Sequence Times Detail"

        wb.save(excel_filename)
        logger.info(f"'{excel_filename}' did not exist and was created with explicit cell-by-cell headers.")
    else:
        logger.info(f"'{excel_filename}' already exists. Headers not modified by initialize_excel_file.")


class UserData(StatesGroup):
    waiting_for_name = State()
    waiting_for_age = State()


class CorsiTestStates(StatesGroup):
    showing_sequence = State()
    waiting_for_user_sequence = State()
    test_completed = State()


async def show_corsi_sequence(original_grid_message: Message, state: FSMContext):
    data = await state.get_data()
    current_sequence_length = data.get('current_sequence_length', 2)
    corsi_chat_id = original_grid_message.chat.id
    corsi_grid_message_id = original_grid_message.message_id

    restart_button_row = [InlineKeyboardButton(text="🔄", callback_data="corsi_restart_current_test")]

    button_indices = list(range(9))
    random.shuffle(button_indices)
    correct_sequence = button_indices[:current_sequence_length]

    await state.update_data(correct_sequence=correct_sequence, user_input_sequence=[])

    base_buttons = []
    for i in range(9):
        base_buttons.append(InlineKeyboardButton(text="🟣", callback_data=f"corsi_button_{i}"))

    base_keyboard_grid_rows = [base_buttons[i:i + 3] for i in range(0, 9, 3)]

    keyboard_for_base_markup_with_restart = [row[:] for row in base_keyboard_grid_rows]
    keyboard_for_base_markup_with_restart.append(restart_button_row)
    base_markup_with_restart = InlineKeyboardMarkup(inline_keyboard=keyboard_for_base_markup_with_restart)

    await original_grid_message.edit_text("Тест Корси", reply_markup=base_markup_with_restart)
    await state.update_data(corsi_grid_message_id=corsi_grid_message_id, corsi_chat_id=corsi_chat_id)

    corsi_status_message_id = data.get('corsi_status_message_id')
    if not corsi_status_message_id:
        status_message_obj = await bot.send_message(corsi_chat_id, "Приготовитесь...")
        corsi_status_message_id = status_message_obj.message_id
        await state.update_data(corsi_status_message_id=corsi_status_message_id)
    else:
        try:
            await bot.edit_message_text(text="Приготовитесь...", chat_id=corsi_chat_id,
                                        message_id=corsi_status_message_id)
        except TelegramBadRequest:
            status_message_obj = await bot.send_message(corsi_chat_id, "Приготовитесь...")
            corsi_status_message_id = status_message_obj.message_id
            await state.update_data(corsi_status_message_id=corsi_status_message_id)

    await asyncio.sleep(1)
    for i in range(3, 0, -1):
        await bot.edit_message_text(text=f"{i}...", chat_id=corsi_chat_id, message_id=corsi_status_message_id)
        await asyncio.sleep(1)

    await bot.edit_message_text(text="Запоминайте...", chat_id=corsi_chat_id, message_id=corsi_status_message_id)
    await asyncio.sleep(0.5)

    for button_index in correct_sequence:
        flashed_buttons_grid_data_rows = []
        for r_idx in range(3):
            row_buttons = []
            for c_idx in range(3):
                original_button_idx = r_idx * 3 + c_idx
                text_color = "🟡" if original_button_idx == button_index else "🟣"
                row_buttons.append(
                    InlineKeyboardButton(text=text_color, callback_data=f"corsi_button_{original_button_idx}"))
            flashed_buttons_grid_data_rows.append(row_buttons)
        flashed_buttons_grid_data_with_restart = flashed_buttons_grid_data_rows + [list(restart_button_row)]

        flashed_markup = InlineKeyboardMarkup(inline_keyboard=flashed_buttons_grid_data_with_restart)
        await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=corsi_grid_message_id,
                                            reply_markup=flashed_markup)
        await asyncio.sleep(0.5)
        await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=corsi_grid_message_id,
                                            reply_markup=base_markup_with_restart)
        await asyncio.sleep(0.2)

    await bot.edit_message_text(text="Повторите последовательность:", chat_id=corsi_chat_id,
                                message_id=corsi_status_message_id)

    await state.update_data(sequence_start_time=time.time())
    await state.set_state(CorsiTestStates.waiting_for_user_sequence)


@dp.callback_query(F.data.startswith("corsi_button_"), CorsiTestStates.waiting_for_user_sequence)
async def handle_corsi_button_press(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    button_index = int(callback.data.split("_")[-1])

    data = await state.get_data()
    user_input_sequence = data.get('user_input_sequence', [])
    correct_sequence = data.get('correct_sequence', [])
    corsi_grid_message_id = data.get('corsi_grid_message_id')
    corsi_chat_id = data.get('corsi_chat_id')

    if not corsi_grid_message_id or not corsi_chat_id:
        logger.error("Corsi grid message ID or chat ID not found in state for button press.")
        await callback.message.answer("Произошла ошибка, попробуйте начать тест заново через /start.")
        await state.clear()
        return

    user_input_sequence.append(button_index)

    new_buttons_grid_data_rows = []
    for r_idx in range(3):
        row_buttons = []
        for c_idx in range(3):
            original_button_idx = r_idx * 3 + c_idx
            text = "🟡" if original_button_idx in user_input_sequence else "🟣"
            row_buttons.append(InlineKeyboardButton(text=text, callback_data=f"corsi_button_{original_button_idx}"))
        new_buttons_grid_data_rows.append(row_buttons)

    restart_button_row = [InlineKeyboardButton(text="🔄", callback_data="corsi_restart_current_test")]
    new_buttons_grid_data_rows.append(restart_button_row)

    markup = InlineKeyboardMarkup(inline_keyboard=new_buttons_grid_data_rows)

    try:
        await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=corsi_grid_message_id,
                                            reply_markup=markup)
    except TelegramBadRequest as e:
        logger.error(f"Error editing message reply markup on button press: {e}")

    await state.update_data(user_input_sequence=user_input_sequence)

    if len(user_input_sequence) == len(correct_sequence):
        await evaluate_user_sequence(callback.message, state)


@dp.callback_query(F.data == "corsi_restart_current_test",
                   StateFilter(CorsiTestStates.showing_sequence, CorsiTestStates.waiting_for_user_sequence))
async def on_corsi_restart_current_test(callback: CallbackQuery, state: FSMContext):
    await callback.answer(text='Тест Корси перезапущен.', show_alert=True)

    data = await state.get_data()
    corsi_status_message_id = data.get('corsi_status_message_id')
    corsi_grid_message_id = data.get('corsi_grid_message_id')
    corsi_feedback_message_id = data.get('corsi_feedback_message_id')
    current_chat_id = callback.message.chat.id
    fsm_chat_id = data.get('corsi_chat_id', current_chat_id)

    if corsi_status_message_id and fsm_chat_id:
        try:
            await bot.delete_message(chat_id=fsm_chat_id, message_id=corsi_status_message_id)
            logger.info(f"Corsi status message {corsi_status_message_id} deleted on test restart.")
        except TelegramBadRequest as e:
            logger.warning(f"Could not delete status message {corsi_status_message_id} on test restart: {e}")

    if corsi_grid_message_id and fsm_chat_id:
        try:
            await bot.edit_message_text(text="Тест был перезапущен.", chat_id=fsm_chat_id,
                                        message_id=corsi_grid_message_id, reply_markup=None)
            logger.info(f"Corsi grid message {corsi_grid_message_id} edited on test restart.")
        except TelegramBadRequest as e:
            logger.warning(f"Could not edit grid message {corsi_grid_message_id} on test restart: {e}")

    if corsi_feedback_message_id and fsm_chat_id:
        try:
            await bot.delete_message(chat_id=fsm_chat_id, message_id=corsi_feedback_message_id)
            logger.info(f"Corsi feedback message {corsi_feedback_message_id} deleted on test restart.")
        except TelegramBadRequest as e:
            logger.warning(f"Could not delete feedback message {corsi_feedback_message_id} on test restart: {e}")

    await state.clear()
    await bot.send_message(current_chat_id,
                           "Тест был перезапущен. Вы можете начать новый тест из меню (которое появляется после /start).")


async def save_corsi_results(grid_message_ref: Message, state: FSMContext):
    data = await state.get_data()
    telegram_id = grid_message_ref.chat.id

    sequence_times = data.get('sequence_times', [])
    corsi_max_len = max(item['len'] for item in sequence_times) if sequence_times else 0

    corsi_avg_time_per_element = 0.0
    if sequence_times:
        num_valid_sequences = sum(1 for item in sequence_times if item['len'] > 0)
        if num_valid_sequences > 0:
            total_avg_time_sum = sum(item['time'] / item['len'] for item in sequence_times if item['len'] > 0)
            corsi_avg_time_per_element = total_avg_time_sum / num_valid_sequences

    corsi_detail_parts = [f"L{item['len']}:{item['time']:.2f}s" for item in sequence_times]
    corsi_detail_string = "; ".join(corsi_detail_parts)

    if telegram_id in user_data_storage:
        user_data_storage[telegram_id]['corsi_max_len'] = corsi_max_len
        user_data_storage[telegram_id]['corsi_avg_time'] = round(corsi_avg_time_per_element, 2)
        user_data_storage[telegram_id]['corsi_detail'] = corsi_detail_string
    else:
        logger.error(f"User {telegram_id} not found in user_data_storage during save_corsi_results.")

    excel_filename = "persistent_user_data.xlsx"
    try:
        wb = load_workbook(excel_filename)
        ws = wb.active
        updated_row = False
        for row_idx, row_cells in enumerate(ws.iter_rows(min_row=2), start=1):
            if row_cells[0].value == telegram_id:
                ws.cell(row=row_idx, column=5).value = corsi_max_len
                ws.cell(row=row_idx, column=6).value = round(corsi_avg_time_per_element, 2)
                ws.cell(row=row_idx, column=7).value = corsi_detail_string
                updated_row = True
                break
        if updated_row:
            wb.save(excel_filename)
            logger.info(f"Corsi results saved to Excel for user {telegram_id}.")
        else:
            logger.error(f"Failed to find user {telegram_id} in Excel to save Corsi results.")
    except FileNotFoundError:
        logger.error(f"Excel file {excel_filename} not found during save_corsi_results.")
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {e}")

    summary_text = (
        f"Тест Корси завершен!\n"
        f"Максимальная длина последовательности: {corsi_max_len}\n"
        f"Среднее время на элемент: {round(corsi_avg_time_per_element, 2)} сек\n"
        f"Детализация: {corsi_detail_string}"
    )
    await grid_message_ref.answer(summary_text)

    corsi_status_message_id = data.get('corsi_status_message_id')
    corsi_chat_id = data.get('corsi_chat_id')
    corsi_grid_message_id = data.get('corsi_grid_message_id')
    corsi_feedback_message_id = data.get('corsi_feedback_message_id')

    if corsi_status_message_id and corsi_chat_id:
        try:
            await bot.delete_message(chat_id=corsi_chat_id, message_id=corsi_status_message_id)
            logger.info(f"Corsi status message {corsi_status_message_id} deleted.")
        except TelegramBadRequest as e:
            logger.error(f"Error deleting Corsi status message: {e}")

    if corsi_grid_message_id and corsi_chat_id:
        try:
            await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=corsi_grid_message_id,
                                                reply_markup=None)
            logger.info(f"Corsi grid message {corsi_grid_message_id} markup removed.")
        except TelegramBadRequest as e:
            logger.error(f"Error removing markup from Corsi grid message: {e}")

    if corsi_feedback_message_id and corsi_chat_id:
        try:
            await bot.delete_message(chat_id=corsi_chat_id, message_id=corsi_feedback_message_id)
            logger.info(f"Corsi feedback message {corsi_feedback_message_id} deleted.")
        except TelegramBadRequest as e:
            logger.error(f"Error deleting Corsi feedback message: {e}")

    await state.set_state(CorsiTestStates.test_completed)
    await state.update_data(
        current_sequence_length=None, error_count=None, sequence_times=None,
        correct_sequence=None, user_input_sequence=None, sequence_start_time=None,
        corsi_grid_message_id=None, corsi_status_message_id=None, corsi_chat_id=None,
        corsi_feedback_message_id=None
    )
    logger.info(f"Corsi test completed for user {telegram_id}. State set to test_completed and FSM data cleared.")


async def evaluate_user_sequence(grid_message_ref: Message, state: FSMContext):
    data = await state.get_data()
    user_input_sequence = data.get('user_input_sequence', [])
    correct_sequence = data.get('correct_sequence', [])
    current_sequence_length = data.get('current_sequence_length', 2)
    error_count = data.get('error_count', 0)
    sequence_times = data.get('sequence_times', [])
    sequence_start_time = data.get('sequence_start_time', 0)

    corsi_chat_id = data.get('corsi_chat_id') or grid_message_ref.chat.id
    feedback_message_id = data.get('corsi_feedback_message_id')

    time_taken = time.time() - sequence_start_time

    if user_input_sequence == correct_sequence:
        sequence_times.append({'len': current_sequence_length, 'time': time_taken})
        current_sequence_length += 1
        error_count = 0

        feedback_text_bold = "<b>Верно!</b>"
        feedback_text_normal = "Верно!"

        if feedback_message_id is None:
            feedback_msg = await bot.send_message(corsi_chat_id, feedback_text_bold, parse_mode=ParseMode.HTML)
            feedback_message_id = feedback_msg.message_id
            await state.update_data(corsi_feedback_message_id=feedback_message_id)
        else:
            try:
                await bot.edit_message_text(feedback_text_bold, chat_id=corsi_chat_id, message_id=feedback_message_id,
                                            parse_mode=ParseMode.HTML)
            except TelegramBadRequest as e:
                logger.error(f"Error editing feedback to bold 'Верно!': {e}")

        await asyncio.sleep(0.5)

        if feedback_message_id:
            try:
                await bot.edit_message_text(feedback_text_normal, chat_id=corsi_chat_id, message_id=feedback_message_id,
                                            parse_mode=None)
            except TelegramBadRequest as e:
                logger.error(f"Error editing feedback to normal 'Верно!': {e}")

        if current_sequence_length > 9:
            await save_corsi_results(grid_message_ref, state)
        else:
            await state.update_data(
                current_sequence_length=current_sequence_length, error_count=error_count,
                sequence_times=sequence_times, user_input_sequence=[]
            )
            await show_corsi_sequence(grid_message_ref, state)
    else:  # Incorrect sequence
        error_count += 1
        feedback_text_error = "<b>Ошибка! Попробуйте ещё раз</b>"

        if feedback_message_id is None:
            feedback_msg = await bot.send_message(corsi_chat_id, feedback_text_error, parse_mode=ParseMode.HTML)
            feedback_message_id = feedback_msg.message_id
            await state.update_data(corsi_feedback_message_id=feedback_message_id)
        else:
            try:
                await bot.edit_message_text(feedback_text_error, chat_id=corsi_chat_id, message_id=feedback_message_id,
                                            parse_mode=ParseMode.HTML)
            except TelegramBadRequest as e:
                logger.error(f"Error editing feedback to 'Ошибка!': {e}")

        if error_count >= 2:
            await save_corsi_results(grid_message_ref, state)
        else:
            await state.update_data(error_count=error_count, user_input_sequence=[])
            await show_corsi_sequence(grid_message_ref, state)


@dp.message(Command("start"))
async def startmessage(message: Message, state: FSMContext):
    await state.set_state(UserData.waiting_for_name)
    await message.answer('Привет! Давайте начнем. Как вас зовут?')


@dp.message(UserData.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(UserData.waiting_for_age)
    await message.answer('Отлично! Теперь введите ваш возраст.')


@dp.message(UserData.waiting_for_age)
async def process_age(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Пожалуйста, введите возраст цифрами.")
        return

    await state.update_data(age=message.text)

    data = await state.get_data()
    user_name = data.get('name')
    user_age = data.get('age')
    telegram_id = message.from_user.id

    excel_filename = "persistent_user_data.xlsx"

    wb = load_workbook(excel_filename)
    ws = wb.active
    existing_ids = set()
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row and len(row) > 1 and row[1] is not None:
            existing_ids.add(row[1])

    new_id = None
    while True:
        candidate_id = random.randint(1000000, 9999999)
        if candidate_id not in existing_ids:
            new_id = candidate_id
            break

    user_data_storage[telegram_id] = {
        'unique_id': new_id, 'name': user_name, 'age': user_age, 'telegram_id': telegram_id,
        'corsi_max_len': None, 'corsi_avg_time': None, 'corsi_detail': None
    }

    ws.append([telegram_id, new_id, user_name, user_age, "", "", ""])
    wb.save(excel_filename)

    await state.clear()

    id_confirmation_text = f"Спасибо! Ваши данные сохранены. Ваш уникальный номер: {new_id}."
    await message.answer(id_confirmation_text)

    button_run_battery = InlineKeyboardButton(text="Пройти батарею тестов", callback_data="run_test_battery")
    button_select_specific = InlineKeyboardButton(text="Выбрать отдельный тест", callback_data="select_specific_test")
    markup = InlineKeyboardMarkup(inline_keyboard=[[button_run_battery], [button_select_specific]])

    action_selection_text = "Выберите дальнейшее действие:"
    await message.answer(action_selection_text, reply_markup=markup)


@dp.callback_query(F.data == "select_specific_test")
async def on_select_specific_test(callback: CallbackQuery):
    await callback.answer()
    button_corsi_test = InlineKeyboardButton(text="Тест Корси", callback_data="initiate_corsi_test")
    markup = InlineKeyboardMarkup(inline_keyboard=[[button_corsi_test]])
    await callback.message.edit_text("Выберите тест:", reply_markup=markup)


@dp.callback_query(F.data == "run_test_battery")
async def on_run_test_battery(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = callback.from_user.id

    if user_id not in user_data_storage:
        await callback.message.answer("Пожалуйста, сначала зарегистрируйтесь с помощью команды /start.")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        return

    await state.update_data(
        current_sequence_length=2, error_count=0, sequence_times=[],
        correct_sequence=[], user_input_sequence=[], sequence_start_time=0,
        corsi_grid_message_id=None, corsi_status_message_id=None, corsi_chat_id=None,
        corsi_feedback_message_id=None
    )
    await show_corsi_sequence(callback.message, state)


@dp.callback_query(F.data == "initiate_corsi_test")
async def on_initiate_corsi_test(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = callback.from_user.id

    if user_id not in user_data_storage:
        await callback.message.answer("Пожалуйста, сначала зарегистрируйтесь с помощью команды /start.")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        return

    await state.update_data(
        current_sequence_length=2, error_count=0, sequence_times=[],
        correct_sequence=[], user_input_sequence=[], sequence_start_time=0,
        corsi_grid_message_id=None, corsi_status_message_id=None, corsi_chat_id=None,
        corsi_feedback_message_id=None
    )
    await show_corsi_sequence(callback.message, state)


@dp.message(Command("mydata"))
async def show_my_data(message: Message):
    user_id = message.from_user.id
    if user_id in user_data_storage:
        stored_data = user_data_storage[user_id]
        response_text = (
            f"Ваши данные:\n"
            f"Имя: {stored_data['name']}\nВозраст: {stored_data['age']}\n"
            f"Уникальный ID: {stored_data['unique_id']}\n"
            f"Corsi Max Length: {stored_data.get('corsi_max_len', 'N/A')}\n"
            f"Corsi Avg Time: {stored_data.get('corsi_avg_time', 'N/A')}\n"
            f"Corsi Detail: {stored_data.get('corsi_detail', 'N/A')}"
        )
        await message.answer(response_text)
    else:
        await message.answer("Я пока не знаю ваших данных. Пожалуйста, пройдите регистрацию, введя /start.")


@dp.message(Command("export"))
async def export_data_to_excel(message: Message):
    excel_filename = "persistent_user_data.xlsx"
    if os.path.exists(excel_filename):
        document = FSInputFile(excel_filename)
        await message.reply_document(document, caption="Вот текущие данные пользователей в формате Excel.")
    else:
        await message.answer("Файл с данными не найден. Попробуйте перезапустить бота.")


@dp.message(Command("restart"))
async def command_restart_test(message: Message, state: FSMContext):
    current_fsm_state_str = await state.get_state()
    active_corsi_states = [CorsiTestStates.showing_sequence.state, CorsiTestStates.waiting_for_user_sequence.state]

    if current_fsm_state_str in active_corsi_states:
        data = await state.get_data()
        corsi_status_message_id = data.get('corsi_status_message_id')
        fsm_chat_id = data.get('corsi_chat_id')
        corsi_grid_message_id = data.get('corsi_grid_message_id')
        corsi_feedback_message_id = data.get('corsi_feedback_message_id')

        if corsi_status_message_id and fsm_chat_id:
            try:
                await bot.delete_message(chat_id=fsm_chat_id, message_id=corsi_status_message_id)
                logger.info(f"Deleted status message {corsi_status_message_id} in chat {fsm_chat_id} due to /restart.")
            except TelegramBadRequest as e:
                logger.warning(
                    f"Could not delete status message {corsi_status_message_id} in chat {fsm_chat_id} on /restart: {e}")

        if corsi_grid_message_id and fsm_chat_id:
            try:
                await bot.edit_message_text(text="Тест был остановлен командой /restart.", chat_id=fsm_chat_id,
                                            message_id=corsi_grid_message_id, reply_markup=None)
                logger.info(f"Edited grid message {corsi_grid_message_id} in chat {fsm_chat_id} due to /restart.")
            except TelegramBadRequest as e:
                logger.warning(
                    f"Could not edit grid message {corsi_grid_message_id} in chat {fsm_chat_id} on /restart: {e}")

        if corsi_feedback_message_id and fsm_chat_id:
            try:
                await bot.delete_message(chat_id=fsm_chat_id, message_id=corsi_feedback_message_id)
                logger.info(
                    f"Deleted feedback message {corsi_feedback_message_id} in chat {fsm_chat_id} due to /restart.")
            except TelegramBadRequest as e:
                logger.warning(
                    f"Could not delete feedback message {corsi_feedback_message_id} in chat {fsm_chat_id} on /restart: {e}")

        await state.clear()
        await message.answer(
            "Текущий тест был остановлен. Вы можете начать новый тест из меню, которое появляется после регистрации, или пройдя регистрацию заново (/start).")
    else:
        await message.answer(
            "Нет активного теста для перезапуска. Вы можете начать новый тест из меню после команды /start.")


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == '__main__':
    initialize_excel_file()
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        logging.info('Succesfull exit')
