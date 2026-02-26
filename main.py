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
BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")  # 1C: токен Telegram-бота
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")  # 1C: строка подключения к Postgres (Railway)

TABLE_NAME: str = "public.raw_turnover_stock"  # 1C: целевая таблица в БД (сырой слой)

# 1C: синонимы значений да/нет (часто встречаются в отчётах)
TRUE_WORDS = {"1", "true", "True", "TRUE", "да", "Да", "ДА", "yes", "Yes", "Y", "y"}
FALSE_WORDS = {"0", "false", "False", "FALSE", "нет", "Нет", "НЕТ", "no", "No", "N", "n"}

# 1C: КОНТРАКТ колонок.
#     Ключ = название колонки в Excel-отчёте (кириллица, как в файле).
#     Значение = имя колонки в БД (латиница), как "по зелёной стрелке".
#     Важно: названия в Excel считаем строго фиксированными.
RUS_TO_DB: Dict[str, str] = {
    "Номенклатура": "item",
    "Номенклатура.Код": "item_code",
    "Номенклатура.Артикул": "article",
    "Номенклатура.Сегмент номенклатуры": "segment",
    "Номенклатура.Сегмент номенклатуры.Родитель": "pg",
    "Номенклатура.Группа управления запасами": "guz",
    "Номенклатура.Группа аналитического учета": "gau",
    "Номенклатура.Основной менеджер": "manager",
    "Номенклатура.Основной поставщик": "supplier",
    "Неликвид": "nonliq",
    "Н-решение": "n_descn",
    "Средний остаток, шт": "av_stock_qty",
    "Расход, шт": "sales_qty",
    "Выручка": "revenue",
    "Конечный остаток (товары)": "curr_stock_qty",
    "Себестоимость (из отч. себ)": "curr_stock_cost",
    "Себестоимость продаж за период": "sales_cost",
    "Себестоимость среднего остатка": "av_stock_cost",
    "Оборачиваемость, руб": "turns_rub",
    "Свободный остаток текущий": "free_stock_q_ty",
    "Себестоимость свободного остатка": "free_stock_cost",
    "Ранг": "rank_turns",
    "Period": "period",
    "Рзв": "rezerv_qty",
    "Себ.Рзв": "rezerv_cost",
    "Уровень": "level_turns",
    "Вал.Пр": "margin",
    "Рент. %": "prof_pc",
    "Рент.Тов.Зап": "prof_stock",
}

# 1C: набор обязательных колонок после переименования (контроль контракта в коде)
REQUIRED_DB_COLS = set(RUS_TO_DB.values())

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
    # 3B: создаём таблицу под наш фиксированный контракт колонок (если её нет)
    db_exec(
        f"""
        create table if not exists {TABLE_NAME} (
            id bigserial primary key,

            -- 3B: снимок отчёта (из Excel колонки Period -> period)
            period timestamptz not null,

            -- 3B: когда загрузили файл в БД
            loaded_ts timestamptz not null default now(),

            -- 3B: имя исходного Excel-файла
            source_file text,

            -- 3B: товар
            item text,
            item_code text,
            article text,

            -- 3B: классификация/атрибуты
            segment text,
            pg text,                -- parent group / родитель сегмента
            guz text,               -- группа управления запасами
            gau text,               -- группа аналитического учета
            manager text,
            supplier text,

            -- 3B: признаки
            nonliq boolean,
            n_descn text,
            level_turns text,
            rank_turns text,

            -- 3B: метрики (qty / money)
            av_stock_qty numeric,
            sales_qty numeric,
            revenue numeric,
            curr_stock_qty numeric,

            curr_stock_cost numeric,
            sales_cost numeric,
            av_stock_cost numeric,

            turns_rub numeric,

            free_stock_q_ty numeric,
            free_stock_cost numeric,

            rezerv_qty numeric,
            rezerv_cost numeric,

            margin numeric,
            prof_pc numeric,
            prof_stock numeric,

            -- 3B: сырьё целиком (на всякий случай)
            payload jsonb,

            -- 3B: уникальность строки в снимке (один товар в одном периоде)
            constraint ux_raw_turnover_stock unique (period, item_code)
        );
        """
    )

    # 3B: "мягкие миграции" (если таблица когда-то уже создавалась неполной)
    #     Добавляем недостающие колонки без падения.
    db_exec(f"alter table {TABLE_NAME} add column if not exists period timestamptz;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists loaded_ts timestamptz not null default now();")
    db_exec(f"alter table {TABLE_NAME} add column if not exists source_file text;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists item text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists item_code text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists article text;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists segment text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists pg text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists guz text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists gau text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists manager text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists supplier text;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists nonliq boolean;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists n_descn text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists level_turns text;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists rank_turns text;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists av_stock_qty numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists sales_qty numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists revenue numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists curr_stock_qty numeric;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists curr_stock_cost numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists sales_cost numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists av_stock_cost numeric;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists turns_rub numeric;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists free_stock_q_ty numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists free_stock_cost numeric;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists rezerv_qty numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists rezerv_cost numeric;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists margin numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists prof_pc numeric;")
    db_exec(f"alter table {TABLE_NAME} add column if not exists prof_stock numeric;")

    db_exec(f"alter table {TABLE_NAME} add column if not exists payload jsonb;")

    # 3B: уникальный constraint тоже "мягко" не добавляется через IF NOT EXISTS,
    #     поэтому создаём уникальный индекс, если его ещё нет (работает как constraint).
    db_exec(
        f"""
        create unique index if not exists ux_raw_turnover_stock_period_code
        on {TABLE_NAME} (period, item_code);
        """
    )
# ===== 3B END =====


# ===== 3C START =====
def _s(v: Any) -> Optional[str]:
    # 3C: безопасно приводим значение к строке (None/NaN -> None)
    if v is None or pd.isna(v):
        return None
    return str(v)


def upsert_dataframe(df: pd.DataFrame, source_file: str) -> Tuple[int, int]:
    # 3C: основной загрузчик DataFrame -> Postgres (колонки + payload jsonb)
    if df.empty:
        return (0, 0)

    # 3C: 1) переименовываем колонки отчёта по контракту (русские -> DB-имена)
    df = df.rename(columns=RUS_TO_DB)

    # 3C: 2) проверяем, что контракт соблюдён (все обязательные колонки есть)
    missing = sorted(list(REQUIRED_DB_COLS - set(df.columns)))
    if missing:
        raise ValueError(f"Missing required columns after rename: {missing}")

    rows: List[Tuple[Any, ...]] = []

    for i in range(len(df)):
        row = df.iloc[i]

        # 3C: обязательные поля для уникальности снимка
        period = parse_timestamp(row.get("period"))
        item_code = _s(row.get("item_code"))

        if period is None:
            continue
        if not item_code:
            continue

        payload = row_to_payload(row)

        rows.append(
            (
                period,
                source_file,

                _s(row.get("item")),
                item_code,
                _s(row.get("article")),

                _s(row.get("segment")),
                _s(row.get("pg")),
                _s(row.get("guz")),
                _s(row.get("gau")),
                _s(row.get("manager")),
                _s(row.get("supplier")),

                parse_bool(row.get("nonliq")),
                _s(row.get("n_descn")),
                _s(row.get("level_turns")),
                _s(row.get("rank_turns")),

                parse_numeric(row.get("av_stock_qty")),
                parse_numeric(row.get("sales_qty")),
                parse_numeric(row.get("revenue")),
                parse_numeric(row.get("curr_stock_qty")),

                parse_numeric(row.get("curr_stock_cost")),
                parse_numeric(row.get("sales_cost")),
                parse_numeric(row.get("av_stock_cost")),

                parse_numeric(row.get("turns_rub")),

                parse_numeric(row.get("free_stock_q_ty")),
                parse_numeric(row.get("free_stock_cost")),

                parse_numeric(row.get("rezerv_qty")),
                parse_numeric(row.get("rezerv_cost")),

                parse_numeric(row.get("margin")),
                parse_numeric(row.get("prof_pc")),
                parse_numeric(row.get("prof_stock")),

                Jsonb(payload),
            )
        )

    sql = f"""
    insert into {TABLE_NAME} (
        period, source_file,

        item, item_code, article,

        segment, pg, guz, gau, manager, supplier,

        nonliq, n_descn, level_turns, rank_turns,

        av_stock_qty, sales_qty, revenue, curr_stock_qty,

        curr_stock_cost, sales_cost, av_stock_cost,

        turns_rub,

        free_stock_q_ty, free_stock_cost,

        rezerv_qty, rezerv_cost,

        margin, prof_pc, prof_stock,

        payload
    )
    values (
        %s, %s,

        %s, %s, %s,

        %s, %s, %s, %s, %s, %s,

        %s, %s, %s, %s,

        %s, %s, %s, %s,

        %s, %s, %s,

        %s,

        %s, %s,

        %s, %s,

        %s, %s, %s,

        %s
    )
    on conflict (period, item_code)
    do update set
        loaded_ts = now(),
        source_file = excluded.source_file,

        item = excluded.item,
        article = excluded.article,

        segment = excluded.segment,
        pg = excluded.pg,
        guz = excluded.guz,
        gau = excluded.gau,
        manager = excluded.manager,
        supplier = excluded.supplier,

        nonliq = excluded.nonliq,
        n_descn = excluded.n_descn,
        level_turns = excluded.level_turns,
        rank_turns = excluded.rank_turns,

        av_stock_qty = excluded.av_stock_qty,
        sales_qty = excluded.sales_qty,
        revenue = excluded.revenue,
        curr_stock_qty = excluded.curr_stock_qty,

        curr_stock_cost = excluded.curr_stock_cost,
        sales_cost = excluded.sales_cost,
        av_stock_cost = excluded.av_stock_cost,

        turns_rub = excluded.turns_rub,

        free_stock_q_ty = excluded.free_stock_q_ty,
        free_stock_cost = excluded.free_stock_cost,

        rezerv_qty = excluded.rezerv_qty,
        rezerv_cost = excluded.rezerv_cost,

        margin = excluded.margin,
        prof_pc = excluded.prof_pc,
        prof_stock = excluded.prof_stock,

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