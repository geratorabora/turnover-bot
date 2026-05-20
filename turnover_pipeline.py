# ===== 1A START =====
from __future__ import annotations  # 1A: современная типизация

import csv  # 1A: запись CSV без DBeaver
import sys  # 1A: подключение соседнего проекта csv_to_exel
from dataclasses import dataclass  # 1A: простой результат pipeline
from datetime import datetime  # 1A: дата отчёта для второго листа
from pathlib import Path  # 1A: пути к файлам
from typing import Optional  # 1A: опциональные значения

import pandas as pd  # 1A: batch-выгрузка в отдельный лист
import psycopg  # 1A: PostgreSQL
from openpyxl import Workbook  # 1A: отдельный xlsx только для вкладки по сериям
# ===== 1A END =====


# ===== 1B START =====
BOT_DIR = Path(__file__).resolve().parent  # 1B: папка turnover_bot
PROJECT_ROOT = BOT_DIR.parent  # 1B: общий корень проекта
LOCAL_EXPORT_SQL_PATH = BOT_DIR / "turnover_export.sql"  # 1B: SQL для Railway-репозитория turnover_bot
CSV_TO_EXEL_DIR = PROJECT_ROOT / "csv_to_exel"  # 1B: папка существующего Excel-конвертера
SHARED_EXPORT_SQL_PATH = CSV_TO_EXEL_DIR / "turnover_export.sql"  # 1B: SQL в соседнем локальном проекте
DEFAULT_EXPORT_SQL_PATH = LOCAL_EXPORT_SQL_PATH if LOCAL_EXPORT_SQL_PATH.exists() else SHARED_EXPORT_SQL_PATH  # 1B: выбираем доступный SQL
OUTPUT_XLSX_NAME = "turnover_pretty.xlsx"  # 1B: имя готового файла для пользователя
OUTPUT_BATCH_XLSX_NAME = "turnover_batch_stock.xlsx"  # 1B: отдельный файл по вкладке по сериям
OUTPUT_CSV_NAME = "turnover.csv"  # 1B: имя промежуточного CSV
# ===== 1B END =====


# ===== 1C START =====
if str(BOT_DIR) not in sys.path:  # 1C: сначала гарантируем приоритет локального модуля из turnover_bot
    sys.path.insert(0, str(BOT_DIR))  # 1C: добавляем текущую папку первой в sys.path
if CSV_TO_EXEL_DIR.exists() and str(CSV_TO_EXEL_DIR) not in sys.path:  # 1C: соседний проект оставляем только как запасной источник
    sys.path.append(str(CSV_TO_EXEL_DIR))  # 1C: добавляем его в конец, чтобы локальная копия имела приоритет

from csv_to_xlsx_turnover import add_batch_stock_sheet, convert_turnover_csv_to_xlsx  # noqa: E402  # 1C: prettifier c приоритетом локальной версии
# ===== 1C END =====


# ===== 2A START =====
@dataclass
class TurnoverPipelineResult:
    csv_path: Path  # 2A: куда записали turnover.csv
    xlsx_path: Path  # 2A: куда записали turnover_pretty.xlsx
    batch_xlsx_path: Optional[Path]  # 2A: куда записали отдельный xlsx по сериям
    exported_rows: int  # 2A: сколько строк вернула SQL-выгрузка
# ===== 2A END =====


# ===== 2B START =====
def load_batch_stock_sheet_data(database_url: str, report_date: datetime) -> pd.DataFrame:
    """
    2B: Загружаем данные для дополнительного листа по сериям.
    Берём batch-остатки и подтягиваем среднюю себестоимость из turnover-таблицы.
    """

    sql = """
    with turnover_unit_cost as (
        select
            period::date as report_dt,
            item_code,
            max(trim(item)) as item,
            max(trim(article)) as article,
            coalesce(nullif(trim(max(article)), ''), item_code) as article_key,
            sum(curr_stock_qty) as turnover_qty,
            sum(curr_stock_cost) as turnover_cost,
            case
                when nullif(sum(curr_stock_qty), 0) is null then null
                else sum(curr_stock_cost) / nullif(sum(curr_stock_qty), 0)
            end as avg_unit_cost
        from public.raw_turnover_stock
        where period::date = %s
        group by period::date, item_code
    ),
    turnover_article_qty as (
        select
            article_key,
            sum(turnover_qty) as turnover_article_qty
        from turnover_unit_cost
        group by article_key
    ),
    batch_base as (
        select
            b.report_dt,
            b.item,
            b.item_code,
            b.article,
            coalesce(nullif(trim(b.article), ''), b.item_code) as article_key,
            b.quality,
            b.series,
            b.expiry_dt,
            b.batch_stock_qty,
            b.months_on_stock,
            b.estimated_prod_month
        from public.raw_stock_batches b
        where b.report_dt = %s
    ),
    batch_article_qty as (
        select
            article_key,
            sum(batch_stock_qty) as batch_article_qty
        from batch_base
        group by article_key
    )
    select
        coalesce(nullif(trim(b.item), ''), t.item) as "Наименование",
        coalesce(nullif(trim(b.article), ''), t.article) as "Артикул",
        b.quality as "Качество",
        b.series as "Серия",
        b.expiry_dt as "Годен до",
        b.batch_stock_qty as "Остаток по партиям",
        round(t.avg_unit_cost, 2) as "Средняя себестоимость",
        round(b.batch_stock_qty * t.avg_unit_cost, 2) as "Общая себестоимость",
        b.months_on_stock as "Месяцев на складе",
        b.estimated_prod_month as "Оценочный месяц производства",
        ta.turnover_article_qty as turnover_article_qty,
        ba.batch_article_qty as batch_article_qty,
        ba.batch_article_qty - ta.turnover_article_qty as qty_diff
    from batch_base b
    left join turnover_unit_cost t
        on t.report_dt = b.report_dt
       and t.item_code = b.item_code
    left join turnover_article_qty ta
        on ta.article_key = b.article_key
    left join batch_article_qty ba
        on ba.article_key = b.article_key
    order by
        b.months_on_stock desc nulls last,
        round(b.batch_stock_qty * t.avg_unit_cost, 2) desc nulls last,
        coalesce(nullif(trim(b.article), ''), t.article),
        b.quality,
        b.expiry_dt,
        b.series
    """

    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (report_date.date(), report_date.date()))
            rows = cur.fetchall()
            columns = [col.name for col in cur.description]

    df = pd.DataFrame(rows, columns=columns)
    df = df.rename(
        columns={
            "turnover_article_qty": "Общее кол-во по артикулу в отчете по оборачиваемости",
            "batch_article_qty": "Общее кол-во по артикулу в отчете по сериям",
            "qty_diff": "Разница в количестве",
        }
    )
    return df
# ===== 2B END =====


# ===== 2C START =====
def export_batch_stock_xlsx(batch_stock_df: pd.DataFrame, xlsx_path: Path) -> Optional[Path]:
    """
    2C: Собираем отдельный xlsx только с листом по сериям.
    """

    if batch_stock_df is None or batch_stock_df.empty:
        return None

    wb = Workbook()
    wb.remove(wb.active)
    add_batch_stock_sheet(wb=wb, batch_stock_df=batch_stock_df)
    wb.save(xlsx_path)
    return xlsx_path
# ===== 2C END =====


# ===== 3A START =====
def export_turnover_csv(
    database_url: str,
    sql_path: Path,
    csv_path: Path,
) -> int:
    """
    3A: Выполняем SQL-выгрузку напрямую из Python и сохраняем long CSV.
    """

    if not database_url:  # 3A: без строки подключения работать нельзя
        raise RuntimeError("DATABASE_URL is not set")

    if not sql_path.exists():  # 3A: SQL должен быть рядом с проектом
        raise FileNotFoundError(f"SQL export file is missing: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")  # 3A: читаем SQL-выгрузку
    csv_path.parent.mkdir(parents=True, exist_ok=True)  # 3A: создаём временную папку, если нужно

    with psycopg.connect(database_url) as conn:  # 3A: подключаемся к PostgreSQL
        with conn.cursor() as cur:  # 3A: открываем курсор
            cur.execute(sql)  # 3A: выполняем SQL
            columns = [col.name for col in cur.description]  # 3A: берём имена колонок для CSV

            with csv_path.open("w", encoding="utf-8", newline="") as f:  # 3A: открываем CSV для записи
                writer = csv.writer(f, delimiter=";")  # 3A: пишем в формате, который ждёт конвертер
                writer.writerow(columns)  # 3A: первая строка = заголовки

                exported_rows = 0  # 3A: счётчик выгруженных строк
                for row in cur:  # 3A: потоково читаем результат SQL
                    writer.writerow(row)  # 3A: записываем строку в CSV
                    exported_rows += 1  # 3A: обновляем счётчик

    return exported_rows  # 3A: возвращаем размер выгрузки
# ===== 3A END =====


# ===== 4A START =====
def build_turnover_report(
    database_url: str,
    work_dir: Path,
    source_detail_path: Path,
    report_date: Optional[datetime] = None,
    include_batch_sheet: bool = False,
    sql_path: Optional[Path] = None,
) -> TurnoverPipelineResult:
    """
    4A: Полный pipeline после загрузки Excel в БД:
        DB SQL export -> turnover.csv -> turnover_pretty.xlsx.
    """

    export_sql_path = sql_path or DEFAULT_EXPORT_SQL_PATH  # 4A: используем SQL по умолчанию, если не передали другой
    csv_path = work_dir / OUTPUT_CSV_NAME  # 4A: промежуточный CSV во временной папке
    xlsx_path = work_dir / OUTPUT_XLSX_NAME  # 4A: итоговый Excel во временной папке
    batch_xlsx_path = work_dir / OUTPUT_BATCH_XLSX_NAME  # 4A: отдельный xlsx по серии
    batch_stock_df = None  # 4A: по умолчанию дополнительного листа нет

    if include_batch_sheet and report_date is not None:
        batch_stock_df = load_batch_stock_sheet_data(
            database_url=database_url,
            report_date=report_date,
        )

    exported_rows = export_turnover_csv(  # 4A: выполняем SQL и сохраняем CSV
        database_url=database_url,
        sql_path=export_sql_path,
        csv_path=csv_path,
    )

    convert_turnover_csv_to_xlsx(  # 4A: используем существующий Excel prettifier
        csv_path=csv_path,
        xlsx_path=xlsx_path,
        source_detail_path=source_detail_path,
        batch_stock_df=batch_stock_df,
    )

    if not xlsx_path.exists():  # 4A: защита от тихого сбоя генерации
        raise RuntimeError(f"Final workbook was not created: {xlsx_path}")

    exported_batch_xlsx_path = None
    if include_batch_sheet and batch_stock_df is not None and not batch_stock_df.empty:
        exported_batch_xlsx_path = export_batch_stock_xlsx(
            batch_stock_df=batch_stock_df,
            xlsx_path=batch_xlsx_path,
        )

    return TurnoverPipelineResult(  # 4A: возвращаем все важные пути и счётчик
        csv_path=csv_path,
        xlsx_path=xlsx_path,
        batch_xlsx_path=exported_batch_xlsx_path,
        exported_rows=exported_rows,
    )
# ===== 4A END =====
