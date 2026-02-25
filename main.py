import asyncio  # Модуль для запуска асинхронного кода
import logging  # Модуль логирования (чтобы видеть логи в Railway)
import os  # Работа с переменными окружения

from aiogram import Bot, Dispatcher, F  # Bot — Telegram клиент, Dispatcher — обработчики, F — фильтры
from aiogram.types import Message  # Тип входящего сообщения
from dotenv import load_dotenv  # Загрузка .env (локально)

import psycopg  # Библиотека для подключения к PostgreSQL


load_dotenv()  # Загружаем переменные из .env (локально это полезно, в Railway не мешает)


# Берём токен бота из переменной окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Берём строку подключения к PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)  # Включаем логирование

    # Проверяем наличие токена
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=BOT_TOKEN)  # Создаём объект бота
    dp = Dispatcher()  # Создаём диспетчер


    # Команда /start
    @dp.message(F.text == "/start")
    async def start(message: Message):
        await message.answer("Бот запущен. Жду Excel 📊")


    # Команда /db — проверка соединения с базой
    @dp.message(F.text == "/db")
    async def db_check(message: Message):

        # Проверяем: есть ли переменная DATABASE_URL
        if not DATABASE_URL:
            await message.answer("DATABASE_URL не задан.")
            return

        try:
            # Подключаемся к PostgreSQL
            with psycopg.connect(DATABASE_URL) as conn:

                # Создаём курсор для выполнения SQL
                with conn.cursor() as cur:

                    # Проверяем наличие нашей таблицы
                    cur.execute(
                        "select to_regclass('public.raw_turnover_stock');"
                    )

                    result = cur.fetchone()[0]

            await message.answer(f"✅ БД доступна. Таблица: {result}")

        except Exception as e:
            await message.answer(
                f"❌ Ошибка подключения: {type(e).__name__}: {e}"
            )


    await dp.start_polling(bot)  # Запуск бота


if __name__ == "__main__":
    asyncio.run(main())  # Запуск главной функции