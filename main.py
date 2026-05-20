# ===== 1A START =====
import asyncio  # 1A: асинхронный запуск
import logging  # 1A: логирование
import os  # 1A: переменные окружения
import shutil  # 1A: очистка временных папок после двухшаговой загрузки
import tempfile  # 1A: временные папки/файлы
from dataclasses import dataclass  # 1A: модель состояния ожидания второго файла
from datetime import datetime  # 1A: НУЖНО для parse_timestamp/row_to_payload
from pathlib import Path  # 1A: работа с путями
from typing import Any, Dict, List, Optional, Tuple  # 1A: типы

import pandas as pd  # 1A: чтение Excel
import psycopg  # 1A: PostgreSQL
from psycopg.types.json import Jsonb  # 1A: упаковка dict → jsonb для Postgres
from aiogram import Bot, Dispatcher, F  # 1A: aiogram
from aiogram.types import FSInputFile, Message  # 1A: тип сообщений и отправка готового файла
from dotenv import load_dotenv  # 1A: .env
# ===== 1A END =====


# ===== 1B START =====
load_dotenv()  # 1B: грузим .env (локально полезно, в Railway не мешает)
# ===== 1B END =====


# ===== 1C START =====
BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")  # 1C: токен Telegram-бота
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")  # 1C: строка подключения к Postgres (Railway)

TABLE_NAME: str = "public.raw_turnover_stock"  # 1C: целевая таблица в БД (сырой слой)
BATCH_TABLE_NAME: str = "public.raw_stock_batches"  # 1C: целевая таблица второго сырого слоя по сериям
STATEMENT_TABLE_NAME: str = "public.raw_stock_statement"  # 1C: сырой слой ведомости остатков

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

# 1C: КОНТРАКТ колонок второго отчёта "остатки по сериям"
BATCH_RUS_TO_DB: Dict[str, str] = {
    "Номенклатура": "item",
    "Номенклатура.Код": "item_code",
    "Артикул": "article",
    "Номенклатура.Качество": "quality",
    "Номенклатура.Срок годности": "shelf_life_value",
    "Номенклатура.Единица измерения срока годности": "shelf_life_unit",
    "Годен до": "expiry_dt",
    "Серия": "series",
    "Остаточный срок годности (дни)": "residual_shelf_life_days",
    "Месяц": "months_on_stock",
    "Конечный остаток": "batch_stock_qty",
    "Изменение в %": "change_pct",
}
REQUIRED_BATCH_DB_COLS = set(BATCH_RUS_TO_DB.values())

STATEMENT_RUS_TO_DB: Dict[str, str] = {
    "Номенклатура": "item",
    "Ед. изм.": "unit_name",
    "Артикул": "article",
    "Номенклатура.Код": "item_code",
    "Количество": "stock_qty",
}
REQUIRED_STATEMENT_DB_COLS = set(STATEMENT_RUS_TO_DB.values())

MAIN_REPORT_HINT_COLS = {"Номенклатура.Код", "Оборачиваемость, руб", "Конечный остаток (товары)"}
BATCH_REPORT_HINT_COLS = {"Номенклатура.Код", "Номенклатура.Качество", "Годен до", "Серия", "Конечный остаток"}
STATEMENT_REPORT_HINT_COLS = {"Номенклатура.Код", "Количество", "Ед. изм.", "Артикул"}


@dataclass
class PendingUploadSession:
    chat_id: int  # 1C: чат, в котором ждём второй файл
    work_dir: Path  # 1C: временная рабочая папка текущего цикла
    main_report_path: Path  # 1C: путь к загруженному основному отчёту
    main_source_filename: str  # 1C: имя исходного основного файла
    report_date: datetime  # 1C: общая дата отчёта для обеих загрузок
    expected_next: str  # 1C: какой файл ждём следующим: statement или batch


PENDING_UPLOADS: Dict[int, PendingUploadSession] = {}  # 1C: минимальное состояние по чатам

# ===== 1C END =====


# ===== 2A START =====
def normalize_excel_header(name: Any) -> str:
    """
    2A: Нормализуем заголовок Excel перед сопоставлением с RUS_TO_DB.
        Что делаем:
        - None -> ""
        - переводим в строку
        - заменяем неразрывные пробелы на обычные
        - убираем пробелы по краям
        - схлопываем повторные пробелы внутри текста
    """
    if name is None:
        return ""

    s = str(name)

    # заменяем "невидимые" пробелы Excel/1C на обычный пробел
    s = s.replace("\u00A0", " ").replace("\u202F", " ")

    # убираем пробелы по краям
    s = s.strip()

    # схлопываем двойные/тройные пробелы внутри текста
    s = " ".join(s.split())

    return s


def to_snake_case(name: str) -> str:
    """
    2A: Нормализуем имя поля под payload:
        - пробелы/точки/дефисы -> _
        - убираем двойные __
        - lower()
    """
    if name is None:
        return ""
    s = str(name).strip()

    # заменяем частые разделители на "_"
    for ch in [" ", ".", "-", "/", "\\", "(", ")", "%", "№", ","]:
        s = s.replace(ch, "_")

    # убираем подряд идущие "_"
    while "__" in s:
        s = s.replace("__", "_")

    return s.strip("_").lower()


def parse_bool(v: Any) -> Optional[bool]:
    """
    2A: Парсим флаговые значения.
        Поддержка: 1/0, да/нет, true/false, yes/no, Y/N и т.п.
        Пусто/NaN -> None
    """
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    s = str(v).strip()
    if s == "":
        return None

    if s in TRUE_WORDS:
        return True
    if s in FALSE_WORDS:
        return False

    # на всякий случай: "1.0"/"0.0"
    if s == "1.0":
        return True
    if s == "0.0":
        return False

    return None


def parse_numeric(v: Any) -> Optional[float]:
    """
    2A: Парсер чисел (NaN-safe).
        ВАЖНО: NaN/пусто -> None (тогда в БД будет NULL, а sum() будет работать).
        Поддержка строк: "1 234,56", "1234,56", "1 234,56" (неразрывный пробел),
        а также варианты с точкой.
    """
    if v is None:
        return None

    # pandas/numpy NaN
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # уже число
    if isinstance(v, (int, float)):
        # float('nan') тоже отловим
        if isinstance(v, float) and (v != v):
            return None
        return float(v)

    s = str(v).strip()
    if s == "":
        return None

    # удаляем пробелы/неразрывные пробелы, меняем запятую на точку
    s = s.replace("\u00A0", "").replace("\u202F", "").replace(" ", "")
    s = s.replace(",", ".")

    try:
        num = float(s)
        if num != num:  # NaN
            return None
        return num
    except Exception:
        return None


def parse_timestamp(v: Any) -> Optional["datetime"]:
    """
    2A: Парсим дату/время для period/report_ts.
        Поддержка:
        - datetime / pandas Timestamp
        - Excel serial date
        - строки формата dd-mm-yyyy / dd.mm.yyyy / dd/mm/yyyy
        - строки с временем

        ВАЖНО:
        Для строк сначала пробуем dayfirst=True,
        чтобы '03-08-2026' читалось как 08.03.2026, а не 03.08.2026.
    """
    if v is None:
        return None

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # если это уже Timestamp / datetime / Excel-date, pandas обычно понимает корректно
    if not isinstance(v, str):
        try:
            ts = pd.to_datetime(v, errors="coerce")
            if pd.isna(ts):
                return None
            return ts.to_pydatetime()
        except Exception:
            return None

    s = str(v).strip()
    if s == "":
        return None

    # сначала пробуем "день-месяц-год"
    try:
        ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if not pd.isna(ts):
            return ts.to_pydatetime()
    except Exception:
        pass

    # если не вышло - запасной вариант
    try:
        ts = pd.to_datetime(s, errors="coerce", dayfirst=False)
        if not pd.isna(ts):
            return ts.to_pydatetime()
    except Exception:
        pass

    return None
# ===== 2A END =====


# ===== 2C START =====
def derive_report_date(source_filename: str, df: pd.DataFrame) -> datetime:
    """
    2C: Определяем дату отчёта по имени входного файла.
        Основной вариант: первые 6 цифр в имени исходного файла = YYMMDD.
        Запасной вариант: максимальная дата из колонки Period / Период.
    """

    report_date: Optional[datetime] = None

    # 2C: пробуем достать дату из имени файла, например 260419_...xlsx -> 19.04.2026
    digits = ""
    for ch in source_filename:
        if ch.isdigit():
            digits += ch
            if len(digits) == 6:
                break
        elif digits:
            break

    if len(digits) == 6:
        try:
            report_date = datetime.strptime(digits, "%y%m%d")
        except Exception:
            report_date = None

    # 2C: если в имени даты нет, берём последнюю дату периода из содержимого Excel
    if report_date is None:
        period_col = "Period" if "Period" in df.columns else "Период" if "Период" in df.columns else None
        if period_col is not None:
            parsed_dates = [parse_timestamp(value) for value in df[period_col].dropna().tolist()]
            parsed_dates = [value for value in parsed_dates if value is not None]
            if parsed_dates:
                report_date = max(parsed_dates)

    # 2C: последний запасной вариант нужен только чтобы не уронить отправку файла из-за имени
    if report_date is None:
        report_date = datetime.now()

    return report_date


def build_report_filename_from_date(report_date: datetime) -> str:
    # 2C: Формируем имя итогового Excel из уже известной даты отчёта
    prefix = report_date.strftime("%y%m%d")
    suffix = report_date.strftime("%d_%m_%y")
    return f"{prefix}_оборачиваемость_и_остатки_на_основных_складах_на_{suffix}.xlsx"


def build_report_filename(source_filename: str, df: pd.DataFrame) -> str:
    # 2C: совместимая оболочка для старых вызовов
    return build_report_filename_from_date(derive_report_date(source_filename, df))
# ===== 2C END =====


# ===== 2B START =====
def row_to_payload(row: Any) -> Dict[str, Any]:
    """
    2B: Собираем payload для jsonb.

    Правило:
    - В момент вызова row_to_payload() row уже должен содержать колонки с DB-именами
      (после df.rename(columns=RUS_TO_DB)).
    - payload хранит "сырьё строки" в удобном виде:
        * ключи = snake_case от DB-имени
        * значения = нормализованные Python-типы (None вместо NaN)
    """

    # 2B: приводим вход к dict
    if isinstance(row, pd.Series):
        data = row.to_dict()
    elif isinstance(row, dict):
        data = row
    else:
        # на крайний случай
        try:
            data = dict(row)
        except Exception:
            return {}

    payload: Dict[str, Any] = {}

    for k, v in data.items():
        # 2B: пропускаем пустые ключи
        if k is None:
            continue

        key = to_snake_case(str(k))

        # 2B: NaN/NaT -> None
        try:
            if pd.isna(v):
                payload[key] = None
                continue
        except Exception:
            pass

        # 2B: datetime / Timestamp -> ISO строка (чтобы json был чистый)
        if isinstance(v, (pd.Timestamp, datetime)):
            try:
                payload[key] = v.isoformat()
            except Exception:
                payload[key] = str(v)
            continue

        # 2B: numpy типы / обычные числа
        if isinstance(v, (int, float, bool)):
            # float('nan') (на всякий случай)
            if isinstance(v, float) and (v != v):
                payload[key] = None
            else:
                payload[key] = v
            continue

        # 2B: строки
        if isinstance(v, str):
            s = v.strip()
            payload[key] = s if s != "" else None
            continue

        # 2B: прочие типы (оставляем как есть, если сериализуется)
        payload[key] = v

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


def db_fetchall(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Any]:
    # 3A: выполнить SQL и вернуть все строки
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
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

    db_exec(
        f"""
        create table if not exists {BATCH_TABLE_NAME} (
            id bigserial primary key,
            report_dt date not null,
            loaded_ts timestamptz not null default now(),
            source_file text,

            item text,
            item_code text,
            article text,
            quality text,

            shelf_life_value numeric,
            shelf_life_unit text,
            expiry_dt date,
            series text,

            residual_shelf_life_days numeric,
            months_on_stock numeric,
            estimated_prod_month date,
            batch_stock_qty numeric,
            change_pct numeric,

            payload jsonb,

            constraint ux_raw_stock_batches unique (report_dt, item_code, quality, series, expiry_dt)
        );
        """
    )

    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists report_dt date;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists loaded_ts timestamptz not null default now();")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists source_file text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists item text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists item_code text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists article text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists quality text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists shelf_life_value numeric;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists shelf_life_unit text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists expiry_dt date;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists series text;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists residual_shelf_life_days numeric;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists months_on_stock numeric;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists estimated_prod_month date;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists batch_stock_qty numeric;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists change_pct numeric;")
    db_exec(f"alter table {BATCH_TABLE_NAME} add column if not exists payload jsonb;")

    db_exec(
        f"""
        create unique index if not exists ux_raw_stock_batches_report_code_quality_series_expiry
        on {BATCH_TABLE_NAME} (report_dt, item_code, quality, series, expiry_dt);
        """
    )

    db_exec(
        f"""
        create table if not exists {STATEMENT_TABLE_NAME} (
            id bigserial primary key,
            report_dt date not null,
            loaded_ts timestamptz not null default now(),
            source_file text,

            item text,
            item_code text,
            article text,
            unit_name text,
            stock_qty numeric,
            payload jsonb,

            constraint ux_raw_stock_statement unique (report_dt, item_code)
        );
        """
    )

    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists report_dt date;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists loaded_ts timestamptz not null default now();")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists source_file text;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists item text;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists item_code text;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists article text;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists unit_name text;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists stock_qty numeric;")
    db_exec(f"alter table {STATEMENT_TABLE_NAME} add column if not exists payload jsonb;")

    db_exec(
        f"""
        create unique index if not exists ux_raw_stock_statement_report_code
        on {STATEMENT_TABLE_NAME} (report_dt, item_code);
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

    # 3C: 1) сначала чистим заголовки Excel от хвостовых/невидимых пробелов
    df = df.copy()
    df.columns = [normalize_excel_header(col) for col in df.columns]

    # 3C: 2) переименовываем колонки отчёта по контракту (русские -> DB-имена)
    df = df.rename(columns=RUS_TO_DB)

    # 3C: 3) проверяем, что контракт соблюдён (все обязательные колонки есть)
    missing = sorted(list(REQUIRED_DB_COLS - set(df.columns)))
    if missing:
        actual_cols = list(df.columns)
        raise ValueError(
            f"Missing required columns after rename: {missing}. "
            f"Actual columns after normalize/rename: {actual_cols}"
        )

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


# ===== 3D START =====
def detect_report_kind(df: pd.DataFrame) -> Optional[str]:
    # 3D: распознаём тип отчёта по набору колонок
    normalized_cols = {normalize_excel_header(col) for col in df.columns}
    if MAIN_REPORT_HINT_COLS.issubset(normalized_cols) and ("Period" in normalized_cols or "Период" in normalized_cols):
        return "main"
    if STATEMENT_REPORT_HINT_COLS.issubset(normalized_cols):
        return "statement"
    if BATCH_REPORT_HINT_COLS.issubset(normalized_cols):
        return "batch"
    return None


def estimate_production_month(
    expiry_dt: Optional[datetime],
    shelf_life_value: Optional[float],
    shelf_life_unit: Optional[str],
) -> Optional[datetime]:
    # 3D: оцениваем месяц производства как "годен до" минус общий срок годности
    if expiry_dt is None or shelf_life_value is None:
        return None

    unit = (shelf_life_unit or "").strip().lower()
    amount = int(round(shelf_life_value))
    ts = pd.Timestamp(expiry_dt)

    try:
        if unit.startswith("мес"):
            prod_ts = ts - pd.DateOffset(months=amount)
        elif unit.startswith("год"):
            prod_ts = ts - pd.DateOffset(years=amount)
        elif unit.startswith("дн") or unit.startswith("day"):
            prod_ts = ts - pd.Timedelta(days=amount)
        else:
            return None
    except Exception:
        return None

    return datetime(prod_ts.year, prod_ts.month, 1)


def upsert_batches_dataframe(df: pd.DataFrame, source_file: str, report_date: datetime) -> Tuple[int, int]:
    # 3D: загрузчик второго отчёта по сериям в отдельную raw-таблицу
    if df.empty:
        return (0, 0)

    df = df.copy()
    df.columns = [normalize_excel_header(col) for col in df.columns]
    df = df.rename(columns=BATCH_RUS_TO_DB)

    missing = sorted(list(REQUIRED_BATCH_DB_COLS - set(df.columns)))
    if missing:
        actual_cols = list(df.columns)
        raise ValueError(
            f"Missing required batch columns after rename: {missing}. "
            f"Actual columns after normalize/rename: {actual_cols}"
        )

    report_dt = report_date.date()
    rows: List[Tuple[Any, ...]] = []

    for i in range(len(df)):
        row = df.iloc[i]
        item_code = _s(row.get("item_code"))
        if not item_code:
            continue

        expiry_ts = parse_timestamp(row.get("expiry_dt"))
        expiry_date = expiry_ts.date() if expiry_ts is not None else None
        shelf_life_value = parse_numeric(row.get("shelf_life_value"))
        shelf_life_unit = _s(row.get("shelf_life_unit"))
        estimated_prod_month = estimate_production_month(expiry_ts, shelf_life_value, shelf_life_unit)

        rows.append(
            (
                report_dt,
                source_file,
                _s(row.get("item")),
                item_code,
                _s(row.get("article")),
                _s(row.get("quality")),
                shelf_life_value,
                shelf_life_unit,
                expiry_date,
                _s(row.get("series")),
                parse_numeric(row.get("residual_shelf_life_days")),
                parse_numeric(row.get("months_on_stock")),
                estimated_prod_month.date() if estimated_prod_month is not None else None,
                parse_numeric(row.get("batch_stock_qty")),
                parse_numeric(row.get("change_pct")),
                Jsonb(row_to_payload(row)),
            )
        )

    sql = f"""
    insert into {BATCH_TABLE_NAME} (
        report_dt, source_file, item, item_code, article, quality,
        shelf_life_value, shelf_life_unit, expiry_dt, series,
        residual_shelf_life_days, months_on_stock, estimated_prod_month,
        batch_stock_qty, change_pct, payload
    )
    values (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s
    )
    on conflict (report_dt, item_code, quality, series, expiry_dt)
    do update set
        source_file = excluded.source_file,
        item = excluded.item,
        article = excluded.article,
        shelf_life_value = excluded.shelf_life_value,
        shelf_life_unit = excluded.shelf_life_unit,
        residual_shelf_life_days = excluded.residual_shelf_life_days,
        months_on_stock = excluded.months_on_stock,
        estimated_prod_month = excluded.estimated_prod_month,
        batch_stock_qty = excluded.batch_stock_qty,
        change_pct = excluded.change_pct,
        payload = excluded.payload
    ;
    """

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()

    return (len(df), len(rows))


def upsert_statement_dataframe(df: pd.DataFrame, source_file: str, report_date: datetime) -> Tuple[int, int]:
    # 3D: загрузчик ведомости остатков в отдельную raw-таблицу
    if df.empty:
        return (0, 0)

    df = df.copy()
    df.columns = [normalize_excel_header(col) for col in df.columns]
    df = df.rename(columns=STATEMENT_RUS_TO_DB)

    missing = sorted(list(REQUIRED_STATEMENT_DB_COLS - set(df.columns)))
    if missing:
        actual_cols = list(df.columns)
        raise ValueError(
            f"Missing required statement columns after rename: {missing}. "
            f"Actual columns after normalize/rename: {actual_cols}"
        )

    report_dt = report_date.date()
    rows: List[Tuple[Any, ...]] = []

    for i in range(len(df)):
        row = df.iloc[i]
        item_code = _s(row.get("item_code"))
        stock_qty = parse_numeric(row.get("stock_qty"))
        if not item_code or stock_qty is None:
            continue

        rows.append(
            (
                report_dt,
                source_file,
                _s(row.get("item")),
                item_code,
                _s(row.get("article")),
                _s(row.get("unit_name")),
                stock_qty,
                Jsonb(row_to_payload(row)),
            )
        )

    sql = f"""
    insert into {STATEMENT_TABLE_NAME} (
        report_dt, source_file, item, item_code, article, unit_name, stock_qty, payload
    )
    values (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    on conflict (report_dt, item_code)
    do update set
        source_file = excluded.source_file,
        item = excluded.item,
        article = excluded.article,
        unit_name = excluded.unit_name,
        stock_qty = excluded.stock_qty,
        payload = excluded.payload
    ;
    """

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()

    return (len(df), len(rows))


def cleanup_pending_session(chat_id: int) -> None:
    # 3D: очищаем временные файлы после завершения цикла
    session = PENDING_UPLOADS.pop(chat_id, None)
    if session is None:
        return
    shutil.rmtree(session.work_dir, ignore_errors=True)


async def finalize_and_send_report(message: Message, session: PendingUploadSession, include_batch_sheet: bool) -> None:
    # 3D: общий финализатор после первого или второго шага загрузки
    try:
        from turnover_pipeline import build_turnover_report
    except Exception as e:
        await message.answer(f"❌ Не смог подключить модуль сборки Excel: {type(e).__name__}: {e}")
        cleanup_pending_session(session.chat_id)
        return

    try:
        pipeline_result = build_turnover_report(
            database_url=DATABASE_URL or "",
            work_dir=session.work_dir,
            source_detail_path=session.main_report_path,
            report_date=session.report_date,
            include_batch_sheet=include_batch_sheet,
        )
    except FileNotFoundError as e:
        await message.answer(f"❌ SQL-файл выгрузки не найден: {e}")
        cleanup_pending_session(session.chat_id)
        return
    except psycopg.OperationalError as e:
        await message.answer(f"❌ Ошибка подключения к БД при SQL-выгрузке: {type(e).__name__}: {e}")
        cleanup_pending_session(session.chat_id)
        return
    except Exception as e:
        await message.answer(f"❌ Ошибка сборки turnover_pretty.xlsx: {type(e).__name__}: {e}")
        cleanup_pending_session(session.chat_id)
        return

    try:
        report_filename = build_report_filename_from_date(session.report_date)
        await message.answer_document(
            FSInputFile(pipeline_result.xlsx_path, filename=report_filename),
            caption=(
                f"✅ Готово: {report_filename}\n"
                f"Строк в SQL-выгрузке: {pipeline_result.exported_rows}"
            ),
        )

        if include_batch_sheet and pipeline_result.batch_xlsx_path is not None:
            batch_filename = f"{session.report_date.strftime('%y%m%d')}_остатки_по_сериям_на_{session.report_date.strftime('%d_%m_%y')}.xlsx"
            await message.answer_document(
                FSInputFile(pipeline_result.batch_xlsx_path, filename=batch_filename),
                caption="Отдельно прикладываю вкладку по сериям."
            )

        if pipeline_result.discrepancies_xlsx_path is not None:
            discrepancies_filename = f"{session.report_date.strftime('%y%m%d')}_расхождения_оборачиваемость_и_ведомость_на_{session.report_date.strftime('%d_%m_%y')}.xlsx"
            await message.answer_document(
                FSInputFile(pipeline_result.discrepancies_xlsx_path, filename=discrepancies_filename),
                caption="И отдельно прикладываю перечень расхождений с ведомостью."
            )
    except Exception as e:
        await message.answer(f"❌ Готовый файл создан, но не смог отправить его в Telegram: {type(e).__name__}: {e}")
        cleanup_pending_session(session.chat_id)
        return

    cleanup_pending_session(session.chat_id)
# ===== 3D END =====


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
        await message.answer(
            "Бот запущен. Жду основной Excel по оборачиваемости 📊\n"
            "После него пришли файл с ведомостью по остаткам,\n"
            "а потом — остатки по сериям."
        )
# ===== 4B END =====


# ===== 4C START =====
def register_db_check(dp: Dispatcher) -> None:
    # 4C: /db
    @dp.message(F.text == "/db")
    async def db_check(message: Message) -> None:
        try:
            ensure_schema()
            row = db_fetchone(f"select to_regclass('{TABLE_NAME}');")
            batch_row = db_fetchone(f"select to_regclass('{BATCH_TABLE_NAME}');")
            statement_row = db_fetchone(f"select to_regclass('{STATEMENT_TABLE_NAME}');")
            await message.answer(f"✅ БД доступна. Таблицы: {row[0]}, {statement_row[0]}, {batch_row[0]}")
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

        tmp_root = Path(tempfile.mkdtemp(prefix="turnover_bot_"))
        tmp_path = tmp_root / filename

        try:
            tg_file = await message.bot.get_file(message.document.file_id)
            await message.bot.download_file(tg_file.file_path, destination=tmp_path)

            # ===== 5B START =====
            try:
                df = pd.read_excel(tmp_path)
            except Exception as e:
                await message.answer(f"❌ Не смог прочитать Excel: {type(e).__name__}: {e}")
                shutil.rmtree(tmp_root, ignore_errors=True)
                return

            if df.empty:
                await message.answer("Файл прочитан, но в нём 0 строк.")
                shutil.rmtree(tmp_root, ignore_errors=True)
                return

            cols = [normalize_excel_header(col) for col in df.columns]
            report_kind = detect_report_kind(df)
            # ===== 5B END =====

            # ===== 5C START =====
            if report_kind == "main":
                cleanup_pending_session(message.chat.id)

                try:
                    ensure_schema()
                    total_rows, attempt_rows = upsert_dataframe(df, source_file=filename)
                    report_date = derive_report_date(filename, df)
                except Exception as e:
                    await message.answer(f"❌ Ошибка загрузки в БД: {type(e).__name__}: {e}")
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return

                PENDING_UPLOADS[message.chat.id] = PendingUploadSession(
                    chat_id=message.chat.id,
                    work_dir=tmp_root,
                    main_report_path=tmp_path,
                    main_source_filename=filename,
                    report_date=report_date,
                    expected_next="statement",
                )

                await message.answer(
                    "✅ Основной отчёт загружен в БД.\n"
                    f"Строк в файле: {total_rows}\n"
                    f"Строк к вставке (после фильтров): {attempt_rows}\n"
                    f"Колонок в файле: {len(cols)}\n\n"
                    "Теперь пришли файл \"остатки из ведомости\".\n"
                    "После него я попрошу файл \"остатки по сериям\"."
                )
                return

            if report_kind == "statement":
                session = PENDING_UPLOADS.get(message.chat.id)
                if session is None:
                    await message.answer(
                        "Сначала пришли основной отчёт по оборачиваемости, а уже потом файл \"остатки из ведомости\"."
                    )
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return
                if session.expected_next != "statement":
                    await message.answer(
                        "Сейчас я жду другой шаг. После ведомости можно будет прислать файл \"остатки по сериям\"."
                    )
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return

                try:
                    ensure_schema()
                    total_rows, attempt_rows = upsert_statement_dataframe(
                        df=df,
                        source_file=filename,
                        report_date=session.report_date,
                    )
                except Exception as e:
                    await message.answer(f"❌ Ошибка загрузки ведомости остатков: {type(e).__name__}: {e}")
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return

                session.expected_next = "batch"

                await message.answer(
                    "✅ Ведомость остатков загружена.\n"
                    f"Строк в файле: {total_rows}\n"
                    f"Строк к вставке (после фильтров): {attempt_rows}\n"
                    "Теперь пришли файл \"остатки по сериям\".\n"
                    "Если его сейчас нет, просто напиши: пропустить"
                )
                shutil.rmtree(tmp_root, ignore_errors=True)
                return

            if report_kind == "batch":
                session = PENDING_UPLOADS.get(message.chat.id)
                if session is None:
                    await message.answer(
                        "Сначала пришли основной отчёт по оборачиваемости, потом ведомость остатков, и только потом файл \"остатки по сериям\"."
                    )
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return
                if session.expected_next != "batch":
                    await message.answer(
                        "Сейчас я жду файл \"остатки из ведомости\". После него можно будет прислать отчёт по сериям."
                    )
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return

                try:
                    ensure_schema()
                    total_rows, attempt_rows = upsert_batches_dataframe(
                        df=df,
                        source_file=filename,
                        report_date=session.report_date,
                    )
                except Exception as e:
                    await message.answer(f"❌ Ошибка загрузки отчёта по сериям: {type(e).__name__}: {e}")
                    shutil.rmtree(tmp_root, ignore_errors=True)
                    return

                await message.answer(
                    "✅ Отчёт по сериям тоже загружен.\n"
                    f"Строк в файле: {total_rows}\n"
                    f"Строк к вставке (после фильтров): {attempt_rows}\n"
                    "Собираю итоговый turnover_pretty.xlsx..."
                )
                shutil.rmtree(tmp_root, ignore_errors=True)
                await finalize_and_send_report(message, session, include_batch_sheet=True)
                return

            await message.answer(
                "Файл прочитан, но я не понял его тип.\n"
                "Ожидал основной отчёт по оборачиваемости, ведомость остатков или отчёт \"остатки по сериям\".\n"
                f"Первые колонки: {cols[:8]}"
            )
            shutil.rmtree(tmp_root, ignore_errors=True)
            # ===== 5C END =====
        except Exception:
            shutil.rmtree(tmp_root, ignore_errors=True)
            raise
# ===== 5A END =====


# ===== 5D START =====
def register_skip_batch(dp: Dispatcher) -> None:
    # 5D: текстовые команды внутри двухшагового сценария
    @dp.message(F.text)
    async def handle_text_commands(message: Message) -> None:
        text = (message.text or "").strip().lower()

        if text == "пропустить":
            session = PENDING_UPLOADS.get(message.chat.id)
            if session is None:
                await message.answer("Сейчас нечего пропускать: сначала пришли основной отчёт.")
                return
            if session.expected_next == "statement":
                await message.answer("Ведомость остатков сейчас обязательна. Сначала пришли файл \"остатки из ведомости\".")
                return

            await message.answer("Ок, собираю отчёт без данных по сериям...")
            await finalize_and_send_report(message, session, include_batch_sheet=False)
            return

        if text == "/cancel":
            if message.chat.id in PENDING_UPLOADS:
                cleanup_pending_session(message.chat.id)
                await message.answer("Текущий цикл загрузки отменён. Можно прислать новый основной отчёт.")
            else:
                await message.answer("Сейчас нет активной загрузки, которую нужно отменять.")
# ===== 5D END =====


# ===== 6A START =====
def register_fallback_debug(dp: Dispatcher) -> None:
    # 6A: человеко-понятный fallback вместо сырого DEBUG
    @dp.message()
    async def debug_any(message: Message) -> None:
        if message.chat.id in PENDING_UPLOADS:
            await message.answer(
                "Я сейчас жду следующий файл по цепочке загрузки.\n"
                "Если уже дошли до шага с сериями и его нет, просто напиши: пропустить"
            )
            return

        await message.answer("Пришли, пожалуйста, Excel-файл .xlsx с отчётом.")
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
    register_skip_batch(dp)
    register_fallback_debug(dp)  # потом можно отключить блоком целиком

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
# ===== 6B END =====
