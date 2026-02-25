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
                
                # --- 1) Берём дату снимка из колонки Period ---
                report_ts = pd.to_datetime(df["Period"].iloc[0], errors="coerce")  # Берём первое значение Period и парсим в datetime
                if pd.isna(report_ts):  # Если дату распарсить не удалось
                    await message.answer("❌ Не смог распарсить дату из колонки Period.")  # Сообщаем
                    return  # Выходим

                # --- 2) Готовим подтаблицу из нужных колонок ---
                data = df[[
                    "Номенклатура",
                    "Номенклатура.Код",
                    "Номенклатура.Артикул ",
                    "Ранг",
                    "Рзв",
                ]].copy()  # Берём только нужные колонки и копируем, чтобы не трогать оригинал

                # --- 3) Переименовываем колонки под имена в БД ---
                data = data.rename(columns={  # Переименовываем “как в Excel” → “как в Postgres”
                    "Номенклатура": "nomenclature",
                    "Номенклатура.Код": "nomenclature_code",
                    "Номенклатура.Артикул ": "article",
                    "Ранг": "rank",
                    "Рзв": "reserve_qty",
                })

                # --- 4) Добавляем колонку report_ts во все строки ---
                data["report_ts"] = report_ts  # Вставляем дату снимка в каждую строку

                # --- 5) Чистим значения (чтобы не было NaN и странных типов) ---
                data["nomenclature"] = data["nomenclature"].astype(str)  # На всякий: приводим к строке
                data["nomenclature_code"] = data["nomenclature_code"].astype(str)  # Код тоже в строку (иногда Excel превращает в число)
                data["article"] = data["article"].astype(str)  # Артикул в строку
                data["rank"] = data["rank"].astype(str)  # Ранг в строку
                data["reserve_qty"] = pd.to_numeric(data["reserve_qty"], errors="coerce")  # Резерв приводим к числу, ошибки → NaN

                data = data.where(pd.notnull(data), None)  # Заменяем pandas NaN на None, чтобы psycopg корректно вставил NULL

                # --- 6) Готовим список кортежей для вставки ---
                rows = [  # Собираем список строк, каждая строка — кортеж значений
                    (
                        r["report_ts"],
                        r["nomenclature"],
                        r["nomenclature_code"],
                        r["article"],
                        r["rank"],
                        r["reserve_qty"],
                    )
                    for _, r in data.iterrows()  # Пробегаем по всем строкам DataFrame
                ]

                # --- 7) Пишем в БД пачкой ---
                if not DATABASE_URL:  # Если строка подключения к БД не задана
                    await message.answer("❌ DATABASE_URL не задан, не могу загрузить в БД.")  # Сообщаем
                    return  # Выходим

                inserted = 0  # Счётчик вставленных строк

                try:
                    with psycopg.connect(DATABASE_URL) as conn:  # Открываем соединение с Postgres
                        with conn.cursor() as cur:  # Открываем курсор
                            cur.executemany(  # Вставляем много строк разом
                                """
                                insert into public.raw_turnover_stock
                                    (report_ts, nomenclature, nomenclature_code, article, rank, reserve_qty)
                                values
                                    (%s, %s, %s, %s, %s, %s)
                                on conflict (report_ts, nomenclature_code) do nothing
                                """,
                                rows,  # Передаём наши подготовленные строки
                            )
                        conn.commit()  # Фиксируем транзакцию
                    inserted = len(rows)  # Если дошли сюда — считаем, что попытались вставить все строки
                    await message.answer(f"✅ Загружено строк (попытка): {inserted}\nreport_ts={report_ts}")  # Отчитываемся
                except Exception as e:
                    await message.answer(f"❌ Ошибка загрузки в БД: {type(e).__name__}: {e}")  # Показываем ошибку                

                await message.answer(  # Отправляем краткий отчёт
                    f"✅ Excel прочитан (v2 - будет загрузка в БД).\n"
                    f"Строк: {len(df)}\n"
                    f"Колонок: {len(cols)}\n"
                    f"Первые колонки: {cols[:5]}"
                )
            except Exception as e:  # Если чтение упало
                await message.answer(f"❌ Не смог прочитать Excel: {type(e).__name__}: {e}")  # Сообщаем ошибку

    @dp.message()  # Ловим вообще всё, что не поймали другие хэндлеры
    async def debug_any(message: Message):
        # Соберём признаки сообщения
        has_text = message.text is not None
        has_document = message.document is not None
        has_photo = message.photo is not None
        has_caption = message.caption is not None

        # Сформируем короткий отчёт
        await message.answer(
            "DEBUG:\n"
            f"text={has_text}\n"
            f"document={has_document}\n"
            f"photo={has_photo}\n"
            f"caption={has_caption}\n"
            f"content_type={message.content_type}"
        )

    await dp.start_polling(bot)  # Запускаем polling ПОСЛЕ регистрации всех обработчиков


if __name__ == "__main__":  # Точка входа при запуске файла
    asyncio.run(main())  # Запускаем main() через asyncio