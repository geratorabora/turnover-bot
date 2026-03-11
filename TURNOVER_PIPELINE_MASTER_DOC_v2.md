# TURNOVER PIPELINE — MASTER PROJECT DOCUMENTATION (v2)

Дата обновления: 2026-03

---

# 1. Общая цель проекта

Автоматизировать обработку отчёта по оборачиваемости из 1С и подготовку аналитики.

Pipeline обработки данных:

1C Excel
↓
Telegram Bot (@BV_turns_upload_bot)
↓
Railway (Python / aiogram worker)
↓
Supabase PostgreSQL
↓
DBeaver
↓
CSV export
↓
Python converter (VS Code)
↓
Excel отчёт для заказчика
↓
Apache Superset dashboards (планируется)

---

# 2. Telegram Bot

Имя:

BV_turns_upload_bot

Функции:

* принимает Excel файл
* скачивает файл
* читает pandas / openpyxl
* подготавливает данные
* загружает данные в PostgreSQL

Библиотеки:

aiogram
pandas
psycopg
python-dotenv
openpyxl

---

# 3. Railway

Railway используется для запуска Telegram-бота.

Service:

turnover_bot_worker

GitHub repo:

turnover-bot

Deploy:

git push → Railway auto deploy

Важно:

В режиме polling может работать **только одна копия бота**.

Если одновременно работают:

* Railway бот
* локальный бот

возникает ошибка:

TelegramConflictError

Решение:

останавливать локальный бот.

---

# 4. База данных

Используется:

Supabase PostgreSQL

Project:

dphpilflddfbuqhrnlsg

Основная таблица:

public.raw_turnover_stock

Таблица хранит weekly snapshot отчёта.

Ключ:

(report_ts, nomenclature_code)

---

# 5. SQL витрина

Создана view:

public.v_pg_segment_week_metrics

Метрики:

stock_cost_sum
ob_rub
stock_cost_turns_lt_2

Группировка:

period
pg
segment

---

# 6. Использование DBeaver

DBeaver используется для:

* проверки загрузки данных
* написания SQL
* экспорта CSV
* генерации DDL

Экспорт:

DBeaver → Export → CSV

---

# 7. Постобработка CSV

CSV выгружается из БД и обрабатывается Python-скриптом.

CSV → Python → Excel

Требование заказчика:

1 показатель = 1 вкладка Excel.

---

# 8. Локальная структура проектов

## Проект 1 — Telegram Bot

Папка:

Projects/turnover_bot

Назначение:

Excel → Telegram → Supabase

Основные файлы:

.env
main.py
main_marked.md
requirements.txt
Procfile
turnover_project_summary_1.md
turnover_handoff_2026-02-26.md

Этот проект синхронизируется с GitHub и деплоится на Railway.

---

## Проект 2 — CSV → Excel

Папка:

Projects/csv_to_exel

Назначение:

Supabase → CSV → Excel отчёт

Основные файлы:

csv_to_xlsx_turnover.py
turnover.csv
turnover_pretty.xlsx
convert.xlsx
turnover_v1.sql
turnover.md

Этот проект используется локально для подготовки отчётов.

---

# 9. Правила разработки

## Код комментируется построчно

Каждая строка должна иметь комментарий.

Причина:

пользователь учится программированию.

---

## Код меняется только блоками

Файл main.py размечен блоками:

1A
1B
1C
2A
2B
2C
3A
3B
3C

Если нужно изменить код:

"замени блок 3B целиком".

Никогда не вставлять куски.

---

# 10. Текущий статус

Сейчас работает:

✓ Telegram бот принимает Excel
✓ Excel читается pandas
✓ данные загружаются в Supabase
✓ данные проверяются через DBeaver
✓ CSV экспортируется
✓ CSV конвертируется в Excel

---

# 11. Следующие задачи

1. загрузить новые итерации Excel через бота

2. изменить Excel генератор

Новая структура:

1 лист = 1 показатель

3. подключить Supabase к Apache Superset

4. построить дашборды:

* оборачиваемость
* остатки
* неликвид
* динамика по неделям

---

# 12. Что переносить в новый чат

Для продолжения работы потребуется:

* этот MASTER SUMMARY
* main.py
* main_marked.md
* пример Excel файла
* Railway logs (если понадобится)
