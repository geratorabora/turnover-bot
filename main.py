import asyncio  # Модуль для запуска асинхронного кода
import logging  # Модуль логирования (чтобы видеть логи в Railway)
import os  # Работа с переменными окружения

from aiogram import Bot, Dispatcher, F  # Bot — Telegram клиент, Dispatcher — обработчики, F — фильтры
from aiogram.types import Message  # Тип входящего сообщения
from dotenv import load_dotenv  # Загрузка .env (локально)

import psycopg  # Библиотека для подключения к PostgreSQL

import pandas as pd  # pandas нужен, чтобы читать Excel в DataFrame
from pathlib import Path  # Path удобен для работы с путями/файлами
import tempfile  # tempfile создаёт временные файлы/папки безопасно


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

        # Любой присланный документ (файл)
    @dp.message(F.document)
    async def handle_document(message: Message):
        # Проверяем, что это Excel-файл по расширению (простая защита)
        filename = message.document.file_name  # Имя файла, которое прислал пользователь
        if not filename.lower().endswith(".xlsx"):  # Если расширение не .xlsx
            await message.answer("Пришли, пожалуйста, файл .xlsx")  # Просим правильный формат
            return  # Выходим

        # Создаём временную папку для скачивания файла
        with tempfile.TemporaryDirectory() as tmp_dir:  # Папка удалится автоматически после выхода из блока
            tmp_path = Path(tmp_dir) / filename  # Полный путь, куда сохраним файл

            # Скачиваем файл из Telegram на диск (временный)
            file = await message.bot.get_file(message.document.file_id)  # Получаем путь к файлу на серверах Telegram
            await message.bot.download_file(file.file_path, destination=tmp_path)  # Скачиваем файл локально

            try:
                # Читаем Excel: по умолчанию берётся первый лист
                df = pd.read_excel(tmp_path)  # Загружаем таблицу в DataFrame

                # Получаем список колонок
                cols = list(df.columns)  # Превращаем Index в обычный список

                # Проверяем, что ключевая колонка Period есть
                if "Period" not in cols:
                    await message.answer(
                        "Файл прочитан, но не вижу колонку 'Period'. Проверь лист/структуру отчёта."
                    )
                    return

                # Отвечаем кратким отчётом: сколько строк и первые 5 колонок
                await message.answer(
                    f"✅ Excel прочитан.\n"
                    f"Строк: {len(df)}\n"
                    f"Колонок: {len(cols)}\n"
                    f"Первые колонки: {cols[:5]}"
                )

            except Exception as e:
                # Если чтение Excel упало — покажем тип ошибки и текст
                await message.answer(f"❌ Не смог прочитать Excel: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())  # Запуск главной функции