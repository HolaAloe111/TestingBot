import asyncio
import logging
import random
import os
import time
import config # Assuming this file contains BOT_TOKEN
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
from aiogram.exceptions import TelegramBadRequest

# --- Globals & Constants ---
user_data_storage = {} # In-memory storage for user data (Telegram ID -> user dict)

# Bot and Dispatcher setup
bot = Bot(config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_FILENAME = "persistent_user_data.xlsx"
# Excel Column Mapping (0-indexed for code, 1-indexed for user display)
# A (0): Telegram ID
# B (1): Unique ID
# C (2): Name
# D (3): Age
# E (4): Corsi - Max Correct Sequence Length
# F (5): Corsi - Avg Time Per Element (s)
# G (6): Corsi - Sequence Times Detail

IKB = InlineKeyboardButton # Alias for brevity

# Standard keyboard for action selection after registration/login/cancellation
ACTION_SELECTION_KEYBOARD = InlineKeyboardMarkup(inline_keyboard=[
    [IKB(text="Пройти батарею тестов", callback_data="run_test_battery")],
    [IKB(text="Выбрать отдельный тест", callback_data="select_specific_test")]
])

# --- FSM States ---
class UserData(StatesGroup):
    waiting_for_first_time_response = State()
    waiting_for_name = State()
    waiting_for_age = State()
    waiting_for_unique_id = State()

class CorsiTestStates(StatesGroup):
    showing_sequence = State()
    waiting_for_user_sequence = State()
    test_completed = State() # Indicates test is done, summary shown. Might be cleared soon after.
    waiting_for_overwrite_confirmation_corsi = State()

# --- Helper Functions ---
def initialize_excel_file():
    """Ensures the Excel file exists and has the correct headers."""
    if not os.path.exists(EXCEL_FILENAME):
        wb = Workbook()
        ws = wb.active
        headers = ["Telegram ID", "Unique ID", "Name", "Age",
                   "Corsi - Max Correct Sequence Length", "Corsi - Avg Time Per Element (s)",
                   "Corsi - Sequence Times Detail"]
        ws.append(headers)
        wb.save(EXCEL_FILENAME)
        logger.info(f"'{EXCEL_FILENAME}' did not exist and was created with headers.")
    else:
        logger.info(f"'{EXCEL_FILENAME}' already exists.")

async def cleanup_corsi_messages(state: FSMContext, bot_instance: Bot, final_grid_text: str = None):
    """Cleans up messages associated with a Corsi test instance."""
    data = await state.get_data()
    chat_id = data.get('corsi_chat_id') # This should be set when a test starts

    if not chat_id:
        logger.warning("Attempted to cleanup Corsi messages but no 'corsi_chat_id' in FSM state.")
        return

    message_ids_to_delete = [
        data.get('corsi_status_message_id'),
        data.get('corsi_feedback_message_id')
    ]
    grid_message_id = data.get('corsi_grid_message_id')
    
    # This was added for overwrite confirmation message, might be specific to that flow's cleanup
    original_confirm_message_id = data.get('original_message_id') 

    for msg_id in message_ids_to_delete:
        if msg_id:
            try:
                await bot_instance.delete_message(chat_id=chat_id, message_id=msg_id)
            except TelegramBadRequest as e:
                logger.warning(f"Could not delete message {msg_id} in chat {chat_id}: {e}")
    
    if grid_message_id:
        try:
            text_to_set = final_grid_text if final_grid_text else "Тест Корси завершен или отменен."
            await bot_instance.edit_message_text(text=text_to_set, chat_id=chat_id, message_id=grid_message_id, reply_markup=None)
        except TelegramBadRequest as e:
            logger.warning(f"Could not edit grid message {grid_message_id} in chat {chat_id}: {e}")

    # If cleaning up during/after an overwrite confirmation
    current_fsm_state = await state.get_state()
    if current_fsm_state == CorsiTestStates.waiting_for_overwrite_confirmation_corsi.state and original_confirm_message_id:
        try:
            text_to_set_confirm = final_grid_text if final_grid_text else "Действие отменено."
            await bot_instance.edit_message_text(text=text_to_set_confirm, chat_id=chat_id, message_id=original_confirm_message_id, reply_markup=None)
        except TelegramBadRequest as e:
            logger.warning(f"Could not edit original confirmation message {original_confirm_message_id} in chat {chat_id}: {e}")

async def send_main_action_menu(message_or_callback: [Message, CallbackQuery], text: str = "Выберите дальнейшее действие:"):
    """Sends the main action selection menu."""
    chat_id = message_or_callback.chat.id if isinstance(message_or_callback, Message) else message_or_callback.message.chat.id
    await bot.send_message(chat_id, text, reply_markup=ACTION_SELECTION_KEYBOARD)


# --- Corsi Test Logic ---
async def show_corsi_sequence(trigger_message: Message, state: FSMContext):
    """Displays the Corsi sequence to the user."""
    data = await state.get_data()
    current_sequence_length = data.get('current_sequence_length', 2)
    # corsi_chat_id is crucial and should have been set by the calling function (e.g., _proceed_to_corsi_test)
    corsi_chat_id = data.get('corsi_chat_id', trigger_message.chat.id) 
    grid_message_id_from_state = data.get('corsi_grid_message_id')
    
    restart_button_row = [IKB(text="🔄", callback_data="corsi_restart_current_test")]
    button_indices = list(range(9))
    random.shuffle(button_indices)
    correct_sequence = button_indices[:current_sequence_length]
    await state.update_data(correct_sequence=correct_sequence, user_input_sequence=[])

    base_buttons = [IKB(text="🟣", callback_data=f"corsi_button_{i}") for i in range(9)]
    base_keyboard_grid_rows = [base_buttons[i:i + 3] for i in range(0, 9, 3)]
    keyboard_for_base_markup_with_restart = [row[:] for row in base_keyboard_grid_rows]
    keyboard_for_base_markup_with_restart.append(restart_button_row)
    base_markup_with_restart = InlineKeyboardMarkup(inline_keyboard=keyboard_for_base_markup_with_restart)

    # Ensure a grid message exists
    if grid_message_id_from_state:
        try:
            await bot.edit_message_text(chat_id=corsi_chat_id, message_id=grid_message_id_from_state, text="Тест Корси", reply_markup=base_markup_with_restart)
        except TelegramBadRequest: 
            grid_msg_obj = await bot.send_message(corsi_chat_id, "Тест Корси", reply_markup=base_markup_with_restart)
            grid_message_id_from_state = grid_msg_obj.message_id
            await state.update_data(corsi_grid_message_id=grid_message_id_from_state)
    else:
        grid_msg_obj = await bot.send_message(corsi_chat_id, "Тест Корси", reply_markup=base_markup_with_restart)
        grid_message_id_from_state = grid_msg_obj.message_id
        await state.update_data(corsi_grid_message_id=grid_message_id_from_state)
    
    # Ensure corsi_chat_id is in state for other functions that might not get trigger_message
    await state.update_data(corsi_chat_id=corsi_chat_id) 

    # Status message handling
    corsi_status_message_id = data.get('corsi_status_message_id')
    if not corsi_status_message_id:
        status_obj = await bot.send_message(corsi_chat_id, "Приготовитесь...")
        corsi_status_message_id = status_obj.message_id
        await state.update_data(corsi_status_message_id=corsi_status_message_id)
    else:
        try: await bot.edit_message_text(text="Приготовитесь...", chat_id=corsi_chat_id, message_id=corsi_status_message_id)
        except TelegramBadRequest: 
            status_obj = await bot.send_message(corsi_chat_id, "Приготовитесь...")
            corsi_status_message_id = status_obj.message_id
            await state.update_data(corsi_status_message_id=corsi_status_message_id)

    await asyncio.sleep(1)
    for i in range(3, 0, -1):
        try: await bot.edit_message_text(text=f"{i}...", chat_id=corsi_chat_id, message_id=corsi_status_message_id)
        except TelegramBadRequest: break # If status message is gone, stop countdown
        await asyncio.sleep(1)
    try: await bot.edit_message_text(text="Запоминайте...", chat_id=corsi_chat_id, message_id=corsi_status_message_id)
    except TelegramBadRequest: return # Cannot proceed if status message is gone
    await asyncio.sleep(0.5)

    for button_index in correct_sequence:
        # ... (rest of flashing logic remains the same)
        flashed_buttons_grid_data_rows = []
        for r_idx in range(3):
            row_buttons = []
            for c_idx in range(3):
                original_button_idx = r_idx * 3 + c_idx
                text_color = "🟡" if original_button_idx == button_index else "🟣"
                row_buttons.append(IKB(text=text_color, callback_data=f"corsi_button_{original_button_idx}"))
            flashed_buttons_grid_data_rows.append(row_buttons)
        flashed_buttons_grid_data_with_restart = flashed_buttons_grid_data_rows + [list(restart_button_row)]
        flashed_markup = InlineKeyboardMarkup(inline_keyboard=flashed_buttons_grid_data_with_restart)
        try:
            await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=grid_message_id_from_state, reply_markup=flashed_markup)
            await asyncio.sleep(0.5)
            await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=grid_message_id_from_state, reply_markup=base_markup_with_restart)
            await asyncio.sleep(0.2)
        except TelegramBadRequest: return # Critical error if grid cannot be updated

    try: await bot.edit_message_text(text="Повторите последовательность:", chat_id=corsi_chat_id, message_id=corsi_status_message_id)
    except TelegramBadRequest: return # Cannot proceed if status message is gone
    
    await state.update_data(sequence_start_time=time.time())
    await state.set_state(CorsiTestStates.waiting_for_user_sequence)

@dp.callback_query(F.data.startswith("corsi_button_"), CorsiTestStates.waiting_for_user_sequence)
async def handle_corsi_button_press(callback: CallbackQuery, state: FSMContext):
    # ... (logic remains largely the same, ensure variable names are clear)
    await callback.answer()
    button_index = int(callback.data.split("_")[-1])
    data = await state.get_data()
    user_input_sequence = data.get('user_input_sequence', [])
    correct_sequence = data.get('correct_sequence', [])
    corsi_grid_message_id = data.get('corsi_grid_message_id')
    corsi_chat_id = data.get('corsi_chat_id')

    if not corsi_grid_message_id or not corsi_chat_id:
        logger.error("Corsi grid/chat ID missing in state for button press.")
        await callback.message.answer("Произошла ошибка, попробуйте начать тест заново через /start.")
        await state.clear(); return
        
    user_input_sequence.append(button_index)
    new_buttons_grid_data_rows = []
    for r_idx in range(3):
        row_buttons = []
        for c_idx in range(3):
            original_button_idx = r_idx * 3 + c_idx
            text = "🟡" if original_button_idx in user_input_sequence else "🟣"
            row_buttons.append(IKB(text=text, callback_data=f"corsi_button_{original_button_idx}"))
        new_buttons_grid_data_rows.append(row_buttons)
    new_buttons_grid_data_rows.append([IKB(text="🔄", callback_data="corsi_restart_current_test")])
    markup = InlineKeyboardMarkup(inline_keyboard=new_buttons_grid_data_rows)
    try: await bot.edit_message_reply_markup(chat_id=corsi_chat_id, message_id=corsi_grid_message_id, reply_markup=markup)
    except TelegramBadRequest as e: logger.error(f"Error editing markup on button press: {e}")
    
    await state.update_data(user_input_sequence=user_input_sequence)
    if len(user_input_sequence) == len(correct_sequence):
        await evaluate_user_sequence(callback.message, state) # Pass callback.message for context

@dp.callback_query(F.data == "corsi_restart_current_test",
                   StateFilter(CorsiTestStates.showing_sequence, CorsiTestStates.waiting_for_user_sequence, CorsiTestStates.waiting_for_overwrite_confirmation_corsi))
async def on_corsi_restart_current_test(callback: CallbackQuery, state: FSMContext):
    await callback.answer(text='Тест Корси перезапущен.', show_alert=True)
    await cleanup_corsi_messages(state, bot, final_grid_text="Тест был перезапущен.")
    await state.clear() 
    await bot.send_message(callback.message.chat.id, "Тест был перезапущен. Вы можете начать новый тест из меню (которое появляется после /start).")

async def save_corsi_results(message_context: Message, state: FSMContext):
    data = await state.get_data()
    telegram_id = message_context.chat.id
    
    unique_id = data.get('unique_id')
    if not unique_id: # Fallback
        user_info_from_storage = user_data_storage.get(telegram_id)
        if user_info_from_storage: unique_id = user_info_from_storage.get('unique_id')
    
    if not unique_id:
        logger.error(f"Unique ID not found for user {telegram_id} in save_corsi_results. Cannot save to Excel.")
        # Simplified summary if critical ID is missing
        corsi_max_len_temp = max(len(s) for s in data.get('sequence_times', [{'len':0}])) if data.get('sequence_times') else 0
        summary_text_error = f"Тест Корси завершен (ошибка сохранения ID).\nМакс. длина: {corsi_max_len_temp}"
        await message_context.answer(summary_text_error)
        await cleanup_corsi_messages(state, bot, final_grid_text="Тест завершен с ошибкой ID.")
        await state.clear() # Clear all state on error
        return

    sequence_times = data.get('sequence_times', [])
    corsi_max_len = max(item['len'] for item in sequence_times) if sequence_times else 0
    corsi_avg_time_per_element = 0.0
    if sequence_times:
        valid_sequences = [item for item in sequence_times if item['len'] > 0]
        if valid_sequences:
            total_avg_time_sum = sum(item['time'] / item['len'] for item in valid_sequences)
            corsi_avg_time_per_element = total_avg_time_sum / len(valid_sequences)
    corsi_detail_string = "; ".join([f"L{item['len']}:{item['time']:.2f}s" for item in sequence_times])

    # Update in-memory storage
    if telegram_id not in user_data_storage: user_data_storage[telegram_id] = {} # Ensure base dict exists
    user_data_storage[telegram_id].update({
        'unique_id': unique_id, # Ensure unique_id is also in user_data_storage
        'name': data.get('name', user_data_storage[telegram_id].get('name', 'N/A_save')),
        'age': data.get('age', user_data_storage[telegram_id].get('age', 'N/A_save')),
        'telegram_id': telegram_id,
        'corsi_max_len': corsi_max_len,
        'corsi_avg_time': round(corsi_avg_time_per_element, 2),
        'corsi_detail': corsi_detail_string
    })
    
    try: # Excel update
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        updated_row_in_excel = False
        for row_idx, row_cells_tuple in enumerate(ws.iter_rows(min_row=2), start=2): # XL is 1-indexed, iter_rows gives cells
            if len(row_cells_tuple) > 1 and row_cells_tuple[1].value == unique_id: # Col B (idx 1) for Unique ID
                ws.cell(row=row_idx, column=5).value = corsi_max_len
                ws.cell(row=row_idx, column=6).value = round(corsi_avg_time_per_element, 2)
                ws.cell(row=row_idx, column=7).value = corsi_detail_string
                updated_row_in_excel = True; break
        if not updated_row_in_excel:
            logger.warning(f"UID {unique_id} (TG ID: {telegram_id}) not found in Excel. Appending.")
            current_user_info = user_data_storage.get(telegram_id, {})
            ws.append([telegram_id, unique_id, current_user_info.get('name', 'N/A_append'), current_user_info.get('age', 'N/A_append'),
                       corsi_max_len, round(corsi_avg_time_per_element, 2), corsi_detail_string])
        wb.save(EXCEL_FILENAME)
        logger.info(f"Corsi results saved to Excel for UID {unique_id} (TG ID: {telegram_id}).")
    except FileNotFoundError: logger.error(f"Excel file {EXCEL_FILENAME} not found during save_corsi_results.")
    except Exception as e: logger.error(f"Error saving Corsi results to Excel: {e}")

    summary_text = (f"Тест Корси завершен!\nМаксимальная длина последовательности: {corsi_max_len}\n"
                    f"Среднее время на элемент: {round(corsi_avg_time_per_element, 2)} сек\nДетализация: {corsi_detail_string}")
    await message_context.answer(summary_text)
    await cleanup_corsi_messages(state, bot, final_grid_text="Тест Корси завершен.")
    await state.clear() # Clear all FSM data for this user as the flow is complete
    logger.info(f"Corsi test flow completed and state cleared for user {telegram_id}.")

async def evaluate_user_sequence(message_context: Message, state: FSMContext):
    # ... (logic remains largely the same, ensure variable names are clear)
    data = await state.get_data()
    corsi_chat_id = data.get('corsi_chat_id', message_context.chat.id)
    user_input_sequence = data.get('user_input_sequence', [])
    correct_sequence = data.get('correct_sequence', [])
    current_sequence_length = data.get('current_sequence_length', 2)
    error_count = data.get('error_count', 0)
    sequence_times = data.get('sequence_times', [])
    sequence_start_time = data.get('sequence_start_time', 0)
    feedback_message_id = data.get('corsi_feedback_message_id')
    time_taken = time.time() - sequence_start_time

    if user_input_sequence == correct_sequence:
        sequence_times.append({'len': current_sequence_length, 'time': time_taken})
        current_sequence_length += 1; error_count = 0
        fb_text_b, fb_text_n = "<b>Верно!</b>", "Верно!"
        if feedback_message_id is None:
            fb_msg = await bot.send_message(corsi_chat_id, fb_text_b, parse_mode=ParseMode.HTML)
            feedback_message_id = fb_msg.message_id
            await state.update_data(corsi_feedback_message_id=feedback_message_id)
        else:
            try: await bot.edit_message_text(fb_text_b, chat_id=corsi_chat_id, message_id=feedback_message_id, parse_mode=ParseMode.HTML)
            except TelegramBadRequest as e: logger.warning(f"Err edit fb bold: {e}")
        await asyncio.sleep(0.5)
        if feedback_message_id:
            try: await bot.edit_message_text(fb_text_n, chat_id=corsi_chat_id, message_id=feedback_message_id, parse_mode=None)
            except TelegramBadRequest as e: logger.warning(f"Err edit fb norm: {e}")
        if current_sequence_length > 9: await save_corsi_results(message_context, state)
        else:
            await state.update_data(current_sequence_length=current_sequence_length, error_count=error_count, sequence_times=sequence_times, user_input_sequence=[])
            await show_corsi_sequence(message_context, state)
    else: # Incorrect sequence
        error_count += 1
        fb_text_err = "<b>Ошибка! Попробуйте ещё раз</b>"
        if feedback_message_id is None:
            fb_msg = await bot.send_message(corsi_chat_id, fb_text_err, parse_mode=ParseMode.HTML)
            feedback_message_id = fb_msg.message_id
            await state.update_data(corsi_feedback_message_id=feedback_message_id)
        else:
            try: await bot.edit_message_text(fb_text_err, chat_id=corsi_chat_id, message_id=feedback_message_id, parse_mode=ParseMode.HTML)
            except TelegramBadRequest as e: logger.warning(f"Err edit fb error: {e}")
        if error_count >= 2: await save_corsi_results(message_context, state)
        else:
            await state.update_data(error_count=error_count, user_input_sequence=[])
            await show_corsi_sequence(message_context, state)

# --- Registration and Main Menu Handlers ---
@dp.message(Command("start"))
async def start_command_handler(message: Message, state: FSMContext):
    await state.clear() # Always clear state on /start for a fresh beginning
    if message.from_user.id in user_data_storage:
        user_info = user_data_storage[message.from_user.id]
        # Store essential info in FSM for potential immediate use by test initiation flows
        await state.update_data(unique_id=user_info.get('unique_id'), name=user_info.get('name'), age=user_info.get('age'))
        await message.answer(f"С возвращением, {user_info.get('name', 'пользователь')}! Ваш ID: {user_info.get('unique_id')}\n"
                             "Выберите дальнейшее действие:", reply_markup=ACTION_SELECTION_KEYBOARD)
        return
    await state.set_state(UserData.waiting_for_first_time_response)
    first_time_kbd = InlineKeyboardMarkup(inline_keyboard=[[IKB(text="Да",cb="user_is_new")],[IKB(text="Нет",cb="user_is_returning")]])
    await message.answer("Вы впервые пользуетесь ботом?", reply_markup=first_time_kbd)

@dp.callback_query(F.data == "user_is_new", UserData.waiting_for_first_time_response)
async def handle_user_is_new_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest as e: logger.info(f"Error removing kbd (user_is_new): {e}")
    await state.set_state(UserData.waiting_for_name)
    await callback.message.answer('Привет! Давайте начнем. Как вас зовут?')

@dp.callback_query(F.data == "user_is_returning", UserData.waiting_for_first_time_response)
async def handle_user_is_returning_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest as e: logger.info(f"Error removing kbd (user_is_returning): {e}")
    await state.set_state(UserData.waiting_for_unique_id)
    await callback.message.answer("Введите ваш уникальный идентификатор")

@dp.message(UserData.waiting_for_unique_id)
async def process_unique_id_input(message: Message, state: FSMContext):
    try: entered_unique_id = int(message.text)
    except ValueError: await message.answer("Пожалуйста, введите корректный числовой идентификатор."); return
    try:
        wb = load_workbook(EXCEL_FILENAME); ws = wb.active; user_found = False
        for row_values in ws.iter_rows(min_row=2, values_only=True): # values_only=True for direct access
            if row_values and len(row_values) > 6 and row_values[1] == entered_unique_id: # Unique ID in col B (idx 1)
                # Columns: TG ID, Unique ID, Name, Age, CorsiMax, CorsiAvg, CorsiDetail
                tg_id_excel, name_excel, age_excel = row_values[0], row_values[2], str(row_values[3])
                c_max, c_avg, c_det = row_values[4], row_values[5], row_values[6]
                
                user_data_storage[message.from_user.id] = {
                    'unique_id': entered_unique_id, 'name': name_excel, 'age': age_excel, 
                    'telegram_id': message.from_user.id, # Use current TG ID
                    'corsi_max_len': c_max, 'corsi_avg_time': float(c_avg) if c_avg is not None else None, 
                    'corsi_detail': c_det
                }
                await state.update_data(**user_data_storage[message.from_user.id]) # Put all into FSM
                await state.clear() # Clear UserData states, user is now "logged in"
                await message.answer(f"Рады снова видеть вас, {name_excel}!")
                await send_main_action_menu(message)
                user_found = True; break
        if not user_found:
            id_not_found_kbd = InlineKeyboardMarkup(inline_keyboard=[[IKB(text="Попробовать снова",cb="try_id_again")],[IKB(text="Зарегистрироваться как новый",cb="register_new_after_fail")]])
            await message.answer("Уникальный идентификатор не найден. Пожалуйста, попробуйте еще раз или зарегистрируйтесь как новый пользователь.", reply_markup=id_not_found_kbd)
    except FileNotFoundError: logger.error(f"{EXCEL_FILENAME} not found."); await message.answer("Ошибка при проверке ID. Попробуйте позже.")
    except Exception as e: logger.error(f"ID check error: {e}"); await message.answer("Произошла непредвиденная ошибка. Попробуйте позже.")

@dp.callback_query(F.data == "try_id_again", UserData.waiting_for_unique_id)
async def handle_try_id_again_callback(callback: CallbackQuery, state: FSMContext): # state is UserData.waiting_for_unique_id
    await callback.answer()
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest as e: logger.info(f"Error removing kbd (try_id_again): {e}")
    await callback.message.answer("Введите ваш уникальный идентификатор") # State remains waiting_for_unique_id

@dp.callback_query(F.data == "register_new_after_fail", UserData.waiting_for_unique_id)
async def handle_register_new_after_fail_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest as e: logger.info(f"Error removing kbd (register_new_after_fail): {e}")
    await state.set_state(UserData.waiting_for_name) # Transition to new user registration
    await callback.message.answer('Давайте начнем. Как вас зовут?')

@dp.message(UserData.waiting_for_name)
async def process_name_input(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(UserData.waiting_for_age)
    await message.answer('Отлично! Теперь введите ваш возраст.')

@dp.message(UserData.waiting_for_age)
async def process_age_input(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (0 < int(message.text) < 120): 
        await message.answer("Пожалуйста, введите корректный возраст цифрами (например, 25)."); return
    
    await state.update_data(age=message.text)
    data = await state.get_data()
    user_name, user_age = data.get('name'), data.get('age')
    telegram_id = message.from_user.id
    new_unique_id = None
    try: # Generate new unique ID
        wb_check = load_workbook(EXCEL_FILENAME); ws_check = wb_check.active
        existing_ids = {row[1] for row in ws_check.iter_rows(min_row=2,values_only=True) if row and len(row)>1 and row[1] is not None}
        new_unique_id = random.randint(1000000,9999999)
        while new_unique_id in existing_ids: new_unique_id = random.randint(1000000,9999999)
        await state.update_data(unique_id=new_unique_id) # Also save to FSM for immediate use
    except Exception as e: 
        logger.error(f"New ID generation error: {e}"); await message.answer("Ошибка при генерации ID. Регистрация не удалась."); 
        await state.clear(); return
        
    user_data_storage[telegram_id] = {'unique_id':new_unique_id,'name':user_name,'age':user_age,'telegram_id':telegram_id,
                                      'corsi_max_len':None,'corsi_avg_time':None,'corsi_detail':None}
    try: # Save to Excel
        wb = load_workbook(EXCEL_FILENAME); ws = wb.active
        ws.append([telegram_id, new_unique_id, user_name, user_age, None, None, None]); wb.save(EXCEL_FILENAME)
    except Exception as e:
        logger.error(f"Save new user to Excel error: {e}"); await message.answer("Ошибка сохранения данных. Регистрация не удалась.")
        if telegram_id in user_data_storage: del user_data_storage[telegram_id] # Rollback in-memory
        await state.clear(); return
        
    await state.clear() # Clear UserData states, registration complete
    await message.answer(f"Спасибо, {user_name}! Ваши данные сохранены. Ваш уникальный номер: {new_unique_id}.")
    await send_main_action_menu(message)

# --- Test Initiation and Overwrite Confirmation ---
async def _proceed_to_corsi_test(message_context: Message, state: FSMContext):
    """Helper to initialize and start the Corsi test sequence."""
    try: await message_context.edit_text("Подготовка к тесту Корси...", reply_markup=None)
    except TelegramBadRequest as e: 
        logger.info(f"Could not edit message before starting Corsi: {e}. Sending new message.")
        await message_context.answer("Подготовка к тесту Корси...") # Send new if edit fails

    await state.set_state(CorsiTestStates.showing_sequence)
    # Ensure unique_id from UserData FSM (if available from login/reg) is carried to CorsiTestStates FSM
    user_fsm_data = await state.get_data() # Get current FSM data
    current_unique_id = user_fsm_data.get('unique_id')
    if not current_unique_id and message_context.from_user.id in user_data_storage: # Fallback for /start known user
        current_unique_id = user_data_storage[message_context.from_user.id].get('unique_id')

    await state.update_data(
        unique_id=current_unique_id, # Carry over unique_id
        current_sequence_length=2, error_count=0, sequence_times=[],
        correct_sequence=[], user_input_sequence=[], sequence_start_time=0,
        corsi_grid_message_id=None, corsi_status_message_id=None, 
        corsi_chat_id=message_context.chat.id, corsi_feedback_message_id=None
    )
    await show_corsi_sequence(message_context, state)

async def check_corsi_data_and_proceed(trigger_event: [CallbackQuery, Message], state: FSMContext):
    """Checks for existing Corsi data and asks for overwrite confirmation if needed."""
    user_id = trigger_event.from_user.id
    message_context = trigger_event.message if isinstance(trigger_event, CallbackQuery) else trigger_event

    if user_id not in user_data_storage: # Should be populated by /start or login
        await message_context.answer("Пожалуйста, сначала завершите регистрацию или войдите с помощью /start.")
        if isinstance(trigger_event, CallbackQuery):
            try: await message_context.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest: pass
        return

    fsm_data = await state.get_data()
    unique_id = fsm_data.get('unique_id') # Should be set by start_command_handler or process_unique_id
    if not unique_id: # Double check from user_data_storage if somehow not in FSM
        unique_id = user_data_storage.get(user_id, {}).get('unique_id')

    if not unique_id:
        logger.error(f"User {user_id} is missing unique_id for Corsi test initiation. FSM: {fsm_data}, Storage: {user_data_storage.get(user_id)}")
        await message_context.answer("Ошибка: не найден ваш уникальный ID. Пожалуйста, попробуйте /start.")
        return

    corsi_data_exists_in_excel = False
    try:
        wb = load_workbook(EXCEL_FILENAME); ws = wb.active
        for row_values in ws.iter_rows(min_row=2, values_only=True):
            if row_values and len(row_values) > 6 and row_values[1] == unique_id: # Unique ID in Col B
                if row_values[4] is not None or row_values[5] is not None or row_values[6] is not None: # Corsi data in E,F,G
                    corsi_data_exists_in_excel = True; break
    except FileNotFoundError: logger.info(f"{EXCEL_FILENAME} not found during overwrite check (normal if first run).")
    except Exception as e: logger.error(f"Excel read error during Corsi data check: {e}")

    if corsi_data_exists_in_excel:
        confirm_kbd = InlineKeyboardMarkup(inline_keyboard=[[IKB(text="Да",cb="overwrite_corsi_confirm")],[IKB(text="Нет",cb="overwrite_corsi_cancel")]])
        await state.set_state(CorsiTestStates.waiting_for_overwrite_confirmation_corsi)
        # Store details of the message that led to this confirmation
        await state.update_data(original_message_id=message_context.message_id, 
                                original_chat_id=message_context.chat.id,
                                unique_id=unique_id) # Ensure unique_id is in this state too
        try: await message_context.edit_text("У вас есть сохраненные результаты Теста Корси. Перезаписать их?", reply_markup=confirm_kbd)
        except TelegramBadRequest: # If original message cannot be edited (e.g. /start command message)
             await message_context.answer("У вас есть сохраненные результаты Теста Корси. Перезаписать их?", reply_markup=confirm_kbd)
    else:
        await _proceed_to_corsi_test(message_context, state)

@dp.callback_query(F.data == "select_specific_test")
async def on_select_specific_test_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id not in user_data_storage: # Basic check
        await callback.message.answer("Пожалуйста, сначала завершите регистрацию или войдите с помощью /start.")
        try: await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest: pass; return
    test_selection_kbd = InlineKeyboardMarkup(inline_keyboard=[[IKB(text="Тест Корси",cb="initiate_corsi_test")]])
    try: await callback.message.edit_text("Выберите тест:", reply_markup=test_selection_kbd)
    except TelegramBadRequest as e: 
        logger.info(f"Edit failed for select_specific_test: {e}"); 
        await callback.message.answer("Выберите тест:",reply_markup=test_selection_kbd)

@dp.callback_query(F.data == "run_test_battery") # Currently, battery = Corsi
async def on_run_test_battery_callback(callback: CallbackQuery, state: FSMContext): 
    await callback.answer()
    await check_corsi_data_and_proceed(callback, state)

@dp.callback_query(F.data == "initiate_corsi_test")
async def on_initiate_corsi_test_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await check_corsi_data_and_proceed(callback, state)

@dp.callback_query(F.data == "overwrite_corsi_confirm", CorsiTestStates.waiting_for_overwrite_confirmation_corsi)
async def handle_overwrite_corsi_confirm_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await _proceed_to_corsi_test(callback.message, state) # Pass the confirmation message

@dp.callback_query(F.data == "overwrite_corsi_cancel", CorsiTestStates.waiting_for_overwrite_confirmation_corsi)
async def handle_overwrite_corsi_cancel_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Запуск теста Корси отменен.", show_alert=False)
    try: await callback.message.edit_text("Запуск теста Корси отменен. Выберите другое действие.", reply_markup=None)
    except TelegramBadRequest as e: logger.info(f"Error editing msg on Corsi cancel: {e}")
    await state.clear() # Clear CorsiTestStates
    await send_main_action_menu(callback.message) # Send main menu as a new message

# --- Utility Handlers ---
@dp.message(Command("mydata"))
async def show_my_data_command(message: Message, state: FSMContext): # Added state for consistency
    user_id = message.from_user.id
    user_info = user_data_storage.get(user_id)
    
    if not user_info: # Try loading from Excel if not in memory
        try:
            wb = load_workbook(EXCEL_FILENAME); ws = wb.active
            loaded_from_excel = False
            for row_values in ws.iter_rows(min_row=2, values_only=True):
                if row_values and row_values[0] == user_id: # Telegram ID in Col A
                    uid, name, age, cmax, cavg, cdet = row_values[1],row_values[2],str(row_values[3]),row_values[4],row_values[5],row_values[6]
                    user_info = {'unique_id':uid,'name':name,'age':age,'telegram_id':user_id,
                                 'corsi_max_len':cmax,'corsi_avg_time':float(cavg) if cavg is not None else None,'corsi_detail':cdet}
                    user_data_storage[user_id] = user_info # Cache in memory
                    loaded_from_excel = True; break
            if not loaded_from_excel:
                await message.answer("Ваши данные не найдены. Пожалуйста, пройдите регистрацию с помощью команды /start."); return
        except FileNotFoundError: await message.answer("Файл данных не найден. Не могу получить ваши данные."); return
        except Exception as e: logger.error(f"Error loading data for /mydata from Excel: {e}"); await message.answer("Ошибка при загрузке ваших данных."); return

    response_text = (f"Ваши данные {'(из файла)' if loaded_from_excel else ''}:\n"
                     f"Имя: {user_info.get('name','N/A')}\nВозраст: {user_info.get('age','N/A')}\n"
                     f"Уникальный ID: {user_info.get('unique_id','N/A')}\n"
                     f"Corsi Max Length: {user_info.get('corsi_max_len','N/A')}\n"
                     f"Corsi Avg Time: {user_info.get('corsi_avg_time','N/A')}\n"
                     f"Corsi Detail: {user_info.get('corsi_detail','N/A')}")
    await message.answer(response_text)

@dp.message(Command("export"))
async def export_data_to_excel_command(message: Message, state: FSMContext): # Added state for consistency
    if os.path.exists(EXCEL_FILENAME): 
        await message.reply_document(FSInputFile(EXCEL_FILENAME), caption="Вот текущие данные пользователей в формате Excel.")
    else: await message.answer("Файл с данными не найден. Попробуйте перезапустить бота.")

@dp.message(Command("restart"))
async def command_restart_test_handler(message: Message, state: FSMContext):
    current_fsm_state = await state.get_state()
    is_corsi_active = current_fsm_state in [
        CorsiTestStates.showing_sequence.state, 
        CorsiTestStates.waiting_for_user_sequence.state,
        CorsiTestStates.waiting_for_overwrite_confirmation_corsi.state
    ]

    if is_corsi_active:
        await cleanup_corsi_messages(state, bot, final_grid_text="Тест был принудительно остановлен командой /restart.")
        await state.clear() 
        await message.answer("Текущий тест Корси был остановлен. Вы можете начать новый тест или регистрацию с /start.")
    elif current_fsm_state is not None: # Any other active state (e.g., registration)
        await state.clear()
        await message.answer("Ваш текущий процесс (например, регистрация) был остановлен. Начните заново с /start.")
    else: # No state active
        await message.answer("Нет активного теста или процесса для перезапуска. Начните с /start.")

# --- Main Bot Execution ---
async def main():
    initialize_excel_file() # Ensure Excel file exists and has headers on startup
    # Load all existing user data from Excel into memory on startup
    # This helps /start immediately recognize known users without an Excel read
    try:
        wb = load_workbook(EXCEL_FILENAME)
        ws = wb.active
        for row_values in ws.iter_rows(min_row=2, values_only=True):
            if row_values and row_values[0] is not None: # Check if Telegram ID exists
                tg_id, uid, name, age, cmax, cavg, cdet = row_values[0], row_values[1], row_values[2], str(row_values[3]), row_values[4], row_values[5], row_values[6]
                user_data_storage[tg_id] = {
                    'telegram_id': tg_id, 'unique_id': uid, 'name': name, 'age': age,
                    'corsi_max_len': cmax, 'corsi_avg_time': float(cavg) if cavg is not None else None,
                    'corsi_detail': cdet
                }
        logger.info(f"Loaded {len(user_data_storage)} users from Excel into memory.")
    except FileNotFoundError:
        logger.info(f"{EXCEL_FILENAME} not found on startup, will be created on first registration.")
    except Exception as e:
        logger.error(f"Error loading data from Excel on startup: {e}")

    await bot.delete_webhook(drop_pending_updates=True) # Clear any pending updates
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Bot stopped successfully by user (KeyboardInterrupt).')
    except Exception as e: # Catch-all for any other unhandled exceptions during startup or polling
        logging.error(f"Unhandled exception in main execution: {e}", exc_info=True)

[end of main.py]
