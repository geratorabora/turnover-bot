# ===== 1A START =====
import asyncio  # 1A: асинхронный запуск
import logging  # 1A: логирование
import os  # 1A: переменные окружения

import tempfile  # 1A: временные папки/файлы
from pathlib import Path  # 1A: работа с путями
from typing import Any, Dict, List, Optional, Tuple  # 1A: типы

import pandas as pd  # 1A: чтение Excel
import psycopg  # 1A: PostgreSQL
from psycopg.types.json import Jsonb  # 1A: упаковка dict → jsonb для Postgres
from aiogram import Bot, Dispatcher, F  # 1A: aiogram
from aiogram.types import Message  # 1A: тип сообщений
from dotenv import load_dotenv  # 1A: .env
# ===== 1A END =====


# ===== 1B START =====
load_dotenv()  # 1B: грузим .env (локально полезно, в Railway не мешает)
# ===== 1B END =====


# ===== 1C START =====
BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")  # 1C: токен бота
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")  # 1C: строка подключения к БД

TABLE_NAME: str = "public.raw_turnover_stock"  # 1C: куда грузим

# 1C: синонимы значений да/нет (часто встречаются в отчётах)
TRUE_WORDS = {"1", "true", "True", "TRUE", "да", "Да", "ДА", "yes", "Yes", "Y", "y"}
FALSE_WORDS = {"0", "false", "False", "FALSE", "нет", "Нет", "НЕТ", "no", "No", "N", "n"}

# 1C: “плоские” ключи, которые хотим держать отдельными колонками (для быстрых запросов)
#     Остальные 29 полей — всё равно попадут в payload jsonb.
FLAT_COLS: Dict[str, str] = {
    "Period": "report_ts",
    "Период": "report_ts",

    "Номенклатура": "nomenclature",
    "Номенклатура.Код": "nomenclature_code",
    "Номенклатура.Код ": "nomenclature_code",

    "Номенклатура.Артикул": "article",
    "Номенклатура.Артикул ": "article",

    "Ранг": "rank",
    "Рзв": "reserve_qty",
    "Резерв": "reserve_qty",

    "nonliquid": "nonliquid",
    "Неликвид": "nonliquid",
}
# ===== 1C END =====


# ===== 2A START =====
def to_snake_case(s: str) -> str:
    # 2A: нормализуем имя колонки в snake_case (для payload)
    s = str(s).strip()
    s = s.replace(" ", "_")
    s = s.replace(".", "_")
    s = s.replace("-", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.lower()


def parse_bool(v: Any) -> Optional[bool]:
    # 2A: да/нет → bool
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip()
    if s in TRUE_WORDS:
        return True
    if s in FALSE_WORDS:
        return False
    return None


def parse_numeric(v: Any) -> Optional[float]:
    # 2A: число (в т.ч. '1 234,56') → float
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def parse_timestamp(v: Any) -> Optional[pd.Timestamp]:
    # 2A: дата/время → Timestamp
    try:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None
# ===== 2A END =====


# ===== 2B START =====
def row_to_payload(row: pd.Series) -> Dict[str, Any]:
    # 2B: вся строка → dict (ключи snake_case), чтобы хранить все поля отчёта в jsonb
    payload: Dict[str, Any] = {}
    for col_name, value in row.items():
        key = to_snake_case(col_name)
        if isinstance(value, pd.Timestamp):
            payload[key] = value.isoformat()
        elif pd.isna(value):
            payload[key] = None
        else:
            payload[key] = value
    return payload
# ===== 2B END =====


# ===== 3A START =====
def db_connect() -> psycopg.Connection:
    # 3A: соединение с БД
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(DATABASE_URL)


def db_exec(sql: str) -> None:
    # 3A: выполнить SQL без результата
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def db_fetchone(sql: str) -> Any:
    # 3A: выполнить SQL и вернуть одну строку
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()
# ===== 3A END =====


# ===== 3B START =====
def ensure_schema() -> None:
    # 3B: создаём таблицу (если нет) + добавляем нужные колонки (если их нет)
    db_exec(
        f"""
        create table if not exists {TABLE_NAME} (
            id bigserial primary key,
            report_ts timestamptz not null,
            loaded_ts timestamptz not null default now(),
            source_file text,

            nomenclature text,
            nomenclature_code text,
            article text,
            rank text,
            reserve_qty numeric,
            nonliquid boolean,

            payload jsonb,

            constraint ux_raw_turnover_stock unique (report_ts, nomenclature_code)
        );
        """
    )

    # 3B: мягкие миграции (если таблицу создали раньше без части колонок)
    db_exec(f"alter table {TABLE_NAME} add column if not exists loaded_ts timestamptz not null default now();")
    db_exec(f"alter table {TABLE_NAME} add column if not exists source_file text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists nomenclature text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists nomenclature_code text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists article text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists rank text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists reserve_qty numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists nonliquid boolean;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists payload jsonb;")
# ===== 3B END =====


# ===== 3C START =====
def build_flat_fields(df: pd.DataFrame, i: int) -> Dict[str, Any]:
    # 3C: вытаскиваем “плоские” поля (по известным именам колонок)
    row = df.iloc[i]

    # 3C: report_ts пытаемся взять из Period / Период
    report_ts = None
    if "Period" in df.columns:
        report_ts = parse_timestamp(row.get("Period"))
    if report_ts is None and "Период" in df.columns:
        report_ts = parse_timestamp(row.get("Период"))

    # 3C: остальные плоские — просто берём, где возможно
    nomenclature = row.get("Номенклатура") if "Номенклатура" in df.columns else None
    nomenclature_code = row.get("Номенклатура.Код") if "Номенклатура.Код" in df.columns else row.get("Номенклатура.Код ")
    article = row.get("Номенклатура.Артикул ") if "Номенклатура.Артикул " in df.columns else row.get("Номенклатура.Артикул")
    rank = row.get("Ранг") if "Ранг" in df.columns else None
    reserve_qty = parse_numeric(row.get("Рзв")) if "Рзв" in df.columns else parse_numeric(row.get("Резерв"))

    nonliquid = None
    if "nonliquid" in df.columns:
        nonliquid = parse_bool(row.get("nonliquid"))
    if nonliquid is None and "Неликвид" in df.columns:
        nonliquid = parse_bool(row.get("Неликвид"))

    # 3C: приводим строки к str (чтобы не было сюрпризов от Excel-типов)
    def s(v: Any) -> Optional[str]:
        if v is None or pd.isna(v):
            return None
        return str(v)

    return {
        "report_ts": report_ts,
        "nomenclature": s(nomenclature),
        "nomenclature_code": s(nomenclature_code),
        "article": s(article),
        "rank": s(rank),
        "reserve_qty": reserve_qty,
        "nonliquid": nonliquid,
    }


def upsert_dataframe(df: pd.DataFrame, source_file: str) -> Tuple[int, int]:
    # 3C: грузим df в БД: все поля → payload, плоские → отдельные колонки
    if df.empty:
        return (0, 0)

    # 3C: базовая проверка наличия Period
    if "Period" not in df.columns and "Период" not in df.columns:
        raise ValueError("No 'Period' (or 'Период') column found")

    rows: List[Tuple[Any, ...]] = []

    for i in range(len(df)):
        flat = build_flat_fields(df, i)
        payload = row_to_payload(df.iloc[i])

        # 3C: фильтры: должна быть дата и код (иначе уникальность/снимок ломаются)
        if flat["report_ts"] is None:
            continue
        if not flat["nomenclature_code"]:
            continue

        rows.append(
            (
                flat["report_ts"],
                source_file,
                flat["nomenclature"],
                flat["nomenclature_code"],
                flat["article"],
                flat["rank"],
                flat["reserve_qty"],
                flat["nonliquid"],
                Jsonb(payload)d,
            )
        )

    sql = f"""
    insert into {TABLE_NAME}
        (report_ts, source_file, nomenclature, nomenclature_code, article, rank, reserve_qty, nonliquid, payload)
    values
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    on conflict (report_ts, nomenclature_code)
    do update set
        loaded_ts = now(),
        source_file = excluded.source_file,
        nomenclature = excluded.nomenclature,
        article = excluded.article,
        rank = excluded.rank,
        reserve_qty = excluded.reserve_qty,
        nonliquid = excluded.nonliquid,
        payload = excluded.payload
    ;
    """

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()

    return (len(df), len(rows))
# ===== 3C END =====


# ===== 4A START =====
def build_app() -> Dispatcher:
    # 4A: создаём диспетчер
    return Dispatcher()
# ===== 4A END =====


# ===== 4B START =====
def register_start(dp: Dispatcher) -> None:
    # 4B: /start
    @dp.message(F.text == "/start")
    async def start(message: Message) -> None:
        await message.answer("Бот запущен. Жду Excel 📊")
# ===== 4B END =====


# ===== 4C START =====
def register_db_check(dp: Dispatcher) -> None:
    # 4C: /db
    @dp.message(F.text == "/db")
    async def db_check(message: Message) -> None:
        try:
            ensure_schema()
            row = db_fetchone(f"select to_regclass('{TABLE_NAME}');")
            await message.answer(f"✅ БД доступна. Таблица: {row[0]}")
        except Exception as e:
            await message.answer(f"❌ Ошибка БД: {type(e).__name__}: {e}")
# ===== 4C END =====


# ===== 5A START =====
def register_excel_upload(dp: Dispatcher) -> None:
    # 5A: обработчик документов
    @dp.message(F.document)
    async def handle_document(message: Message) -> None:
        filename = message.document.file_name
        if not filename or not filename.lower().endswith(".xlsx"):
            await message.answer("Пришли, пожалуйста, файл .xlsx")
            return

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / filename

            tg_file = await message.bot.get_file(message.document.file_id)
            await message.bot.download_file(tg_file.file_path, destination=tmp_path)

            # ===== 5B START =====
            try:
                df = pd.read_excel(tmp_path)
            except Exception as e:
                await message.answer(f"❌ Не смог прочитать Excel: {type(e).__name__}: {e}")
                return

            if df.empty:
                await message.answer("Файл прочитан, но в нём 0 строк.")
                return

            cols = list(df.columns)

            if "Period" not in cols and "Период" not in cols:
                await message.answer(
                    "Файл прочитан, но не вижу колонку 'Period' (или 'Период').\n"
                    f"Первые колонки: {cols[:8]}"
                )
                return
            # ===== 5B END =====

            # ===== 5C START =====
            try:
                ensure_schema()
                total_rows, attempt_rows = upsert_dataframe(df, source_file=filename)
                await message.answer(
                    "✅ Загрузка завершена.\n"
                    f"Строк в файле: {total_rows}\n"
                    f"Строк к вставке (после фильтров): {attempt_rows}\n"
                    f"Колонок в файле: {len(cols)}"
                )
            except Exception as e:
                await message.answer(f"❌ Ошибка загрузки в БД: {type(e).__name__}: {e}")
                return
            # ===== 5C END =====
# ===== 5A END =====


# ===== 6A START =====
def register_fallback_debug(dp: Dispatcher) -> None:
    # 6A: fallback для отладки
    @dp.message()
    async def debug_any(message: Message) -> None:
        await message.answer(
            "DEBUG:\n"
            f"content_type={message.content_type}\n"
            f"text={message.text is not None}\n"
            f"document={message.document is not None}\n"
            f"photo={message.photo is not None}\n"
            f"caption={message.caption is not None}"
        )
# ===== 6A END =====


# ===== 6B START =====
async def main() -> None:
    # 6B: запуск приложения
    logging.basicConfig(level=logging.INFO)

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=BOT_TOKEN)
    dp = build_app()

    register_start(dp)
    register_db_check(dp)
    register_excel_upload(dp)
    register_fallback_debug(dp)  # потом можно отключить блоком целиком

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
# ===== 6B END =====