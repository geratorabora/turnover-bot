# ===== 1A START =====
from __future__ import annotations  # 1A: современная типизация

import csv  # 1A: запись CSV без DBeaver
import sys  # 1A: подключение соседнего проекта csv_to_exel
from dataclasses import dataclass  # 1A: простой результат pipeline
from pathlib import Path  # 1A: пути к файлам
from typing import Optional  # 1A: опциональные значения

import psycopg  # 1A: PostgreSQL
# ===== 1A END =====


# ===== 1B START =====
BOT_DIR = Path(__file__).resolve().parent  # 1B: папка turnover_bot
PROJECT_ROOT = BOT_DIR.parent  # 1B: общий корень проекта
LOCAL_EXPORT_SQL_PATH = BOT_DIR / "turnover_export.sql"  # 1B: SQL для Railway-репозитория turnover_bot
CSV_TO_EXEL_DIR = PROJECT_ROOT / "csv_to_exel"  # 1B: папка существующего Excel-конвертера
SHARED_EXPORT_SQL_PATH = CSV_TO_EXEL_DIR / "turnover_export.sql"  # 1B: SQL в соседнем локальном проекте
DEFAULT_EXPORT_SQL_PATH = LOCAL_EXPORT_SQL_PATH if LOCAL_EXPORT_SQL_PATH.exists() else SHARED_EXPORT_SQL_PATH  # 1B: выбираем доступный SQL
OUTPUT_XLSX_NAME = "turnover_pretty.xlsx"  # 1B: имя готового файла для пользователя
OUTPUT_CSV_NAME = "turnover.csv"  # 1B: имя промежуточного CSV
# ===== 1B END =====


# ===== 1C START =====
if str(BOT_DIR) not in sys.path:  # 1C: сначала гарантируем приоритет локального модуля из turnover_bot
    sys.path.insert(0, str(BOT_DIR))  # 1C: добавляем текущую папку первой в sys.path
if CSV_TO_EXEL_DIR.exists() and str(CSV_TO_EXEL_DIR) not in sys.path:  # 1C: соседний проект оставляем только как запасной источник
    sys.path.append(str(CSV_TO_EXEL_DIR))  # 1C: добавляем его в конец, чтобы локальная копия имела приоритет

from csv_to_xlsx_turnover import convert_turnover_csv_to_xlsx  # noqa: E402  # 1C: prettifier c приоритетом локальной версии
# ===== 1C END =====


# ===== 2A START =====
@dataclass
class TurnoverPipelineResult:
    csv_path: Path  # 2A: куда записали turnover.csv
    xlsx_path: Path  # 2A: куда записали turnover_pretty.xlsx
    exported_rows: int  # 2A: сколько строк вернула SQL-выгрузка
# ===== 2A END =====


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
    sql_path: Optional[Path] = None,
) -> TurnoverPipelineResult:
    """
    4A: Полный pipeline после загрузки Excel в БД:
        DB SQL export -> turnover.csv -> turnover_pretty.xlsx.
    """

    export_sql_path = sql_path or DEFAULT_EXPORT_SQL_PATH  # 4A: используем SQL по умолчанию, если не передали другой
    csv_path = work_dir / OUTPUT_CSV_NAME  # 4A: промежуточный CSV во временной папке
    xlsx_path = work_dir / OUTPUT_XLSX_NAME  # 4A: итоговый Excel во временной папке

    exported_rows = export_turnover_csv(  # 4A: выполняем SQL и сохраняем CSV
        database_url=database_url,
        sql_path=export_sql_path,
        csv_path=csv_path,
    )

    convert_turnover_csv_to_xlsx(  # 4A: используем существующий Excel prettifier
        csv_path=csv_path,
        xlsx_path=xlsx_path,
        source_detail_path=source_detail_path,
    )

    if not xlsx_path.exists():  # 4A: защита от тихого сбоя генерации
        raise RuntimeError(f"Final workbook was not created: {xlsx_path}")

    return TurnoverPipelineResult(  # 4A: возвращаем все важные пути и счётчик
        csv_path=csv_path,
        xlsx_path=xlsx_path,
        exported_rows=exported_rows,
    )
# ===== 4A END =====
