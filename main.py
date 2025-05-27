import asyncio
import logging
import random
import os
from openpyxl import Workbook, load_workbook
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, ReplyKeyboardMarkup, FSInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

user_data_storage = {}

bot = Bot('7540534218:AAGU_nCv162Qw2Wli5nLYa_awpV-RTp5FHk',
          default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def initialize_excel_file():
    excel_filename = "persistent_user_data.xlsx"
    if not os.path.exists(excel_filename):
        wb = Workbook()
        ws = wb.active
        ws.append(["Telegram ID", "Unique ID", "Name", "Age"])
        wb.save(excel_filename)
        logger.info(f"'{excel_filename}' did not exist and was created with headers.")
    else:
        logger.info(f"'{excel_filename}' already exists.")


class UserData(StatesGroup):
    waiting_for_name = State()
    waiting_for_age = State()


@dp.message(CommandStart)
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
    new_id = random.randint(1000000, 9999999)
    data = await state.get_data()
    user_name = data.get('name')
    user_age = data.get('age')
    telegram_id = message.from_user.id

    user_data_storage[telegram_id] = {
        'unique_id': new_id,
        'name': user_name,
        'age': user_age,
        'telegram_id': telegram_id
    }

    excel_filename = "persistent_user_data.xlsx"
    # No need to check for file existence here anymore for header writing,
    # as initialize_excel_file() handles it at startup.
    wb = load_workbook(excel_filename)
    ws = wb.active
    ws.append([telegram_id, new_id, user_name, user_age])
    wb.save(excel_filename)

    await state.clear()
    await message.answer(f"Спасибо! Ваши данные сохранены. Ваш уникальный номер: {new_id}. Теперь вы можете проходить тесты.")


@dp.message(Command("mydata"))
async def show_my_data(message: Message):
    user_id = message.from_user.id
    if user_id in user_data_storage:
        stored_data = user_data_storage[user_id]
        response_text = (
            f"Ваши данные:\n"
            f"Имя: {stored_data['name']}\n"
            f"Возраст: {stored_data['age']}\n"
            f"Уникальный ID: {stored_data['unique_id']}"
        )
        await message.answer(response_text)
    else:
        await message.answer("Я пока не знаю ваших данных. Пожалуйста, пройдите регистрацию, введя /start.")


@dp.message(Command("export"))
async def export_data_to_excel(message: Message):
    excel_filename = "persistent_user_data.xlsx"
    if os.path.exists(excel_filename): # File should always exist due to initialize_excel_file
        document = FSInputFile(excel_filename)
        await message.reply_document(document, caption="Вот текущие данные пользователей в формате Excel.")
    else:
        # This case should ideally not be reached if initialize_excel_file works correctly
        await message.answer("Файл с данными не найден. Попробуйте перезапустить бота.")


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == '__main__':
    initialize_excel_file()
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        logging.info('Succesfull exit')
