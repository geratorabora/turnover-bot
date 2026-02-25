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

load_dotenv()  # Загружаем переменные из .env (локально полезно, на Railway не мешает)

BOT_TOKEN = os.getenv("BOT_TOKEN")  # Берём токен бота из переменной окружения
DATABASE_URL = os.getenv("DATABASE_URL")  # Берём строку подключения к PostgreSQL из переменной окружения


async def main() -> None:  # Главная асинхронная функция приложения
    logging.basicConfig(level=logging.INFO)  # Включаем логирование

    if not BOT_TOKEN:  # Проверяем наличие токена
        raise RuntimeError("BOT_TOKEN is not set")  # Если токена нет — сразу падаем понятной ошибкой

    bot = Bot(token=BOT_TOKEN)  # Создаём объект бота
    dp = Dispatcher()  # Создаём диспетчер


    @dp.message(F.text == "/start")  # Обработчик команды /start
    async def start(message: Message):  # Функция-обработчик
        await message.answer("Бот запущен. Жду Excel 📊")  # Отвечаем пользователю


    @dp.message(F.text == "/db")  # Обработчик команды /db
    async def db_check(message: Message):  # Функция-обработчик
        if not DATABASE_URL:  # Если переменная DATABASE_URL не задана
            await message.answer("DATABASE_URL не задан.")  # Сообщаем об этом
            return  # Выходим

        try:  # Пытаемся подключиться к БД
            with psycopg.connect(DATABASE_URL) as conn:  # Открываем соединение с PostgreSQL
                with conn.cursor() as cur:  # Открываем курсор
                    cur.execute("select to_regclass('public.raw_turnover_stock');")  # Проверяем, видна ли таблица
                    result = cur.fetchone()[0]  # Берём результат (имя таблицы или None)
            await message.answer(f"✅ БД доступна. Таблица: {result}")  # Сообщаем успех
        except Exception as e:  # Если ошибка
            await message.answer(f"❌ Ошибка подключения: {type(e).__name__}: {e}")  # Показываем её


    @dp.message(F.document)  # Обработчик любого присланного документа (файла)
    async def handle_document(message: Message):  # Функция-обработчик файла
        filename = message.document.file_name  # Берём имя файла
        if not filename.lower().endswith(".xlsx"):  # Проверяем расширение
            await message.answer("Пришли, пожалуйста, файл .xlsx")  # Просим правильный формат
            return  # Выходим

        with tempfile.TemporaryDirectory() as tmp_dir:  # Создаём временную папку
            tmp_path = Path(tmp_dir) / filename  # Формируем путь к файлу внутри временной папки

            file = await message.bot.get_file(message.document.file_id)  # Получаем путь файла на серверах Telegram
            await message.bot.download_file(file.file_path, destination=tmp_path)  # Скачиваем файл во временный путь

            try:  # Пытаемся прочитать Excel
                df = pd.read_excel(tmp_path)  # Читаем Excel в DataFrame (пока берём первый лист)
                cols = list(df.columns)  # Список колонок

                if "Period" not in cols:  # Проверяем наличие ключевой колонки Period
                    await message.answer("Файл прочитан, но не вижу колонку 'Period'. Проверь лист/структуру отчёта.")  # Сообщаем
                    return  # Выходим

                await message.answer(  # Отправляем краткий отчёт
                    f"✅ Excel прочитан.\n"
                    f"Строк: {len(df)}\n"
                    f"Колонок: {len(cols)}\n"
                    f"Первые колонки: {cols[:5]}"
                )
            except Exception as e:  # Если чтение упало
                await message.answer(f"❌ Не смог прочитать Excel: {type(e).__name__}: {e}")  # Сообщаем ошибку

    await dp.start_polling(bot)  # Запускаем polling ПОСЛЕ регистрации всех обработчиков


if __name__ == "__main__":  # Точка входа при запуске файла
    asyncio.run(main())  # Запускаем main() через asyncio