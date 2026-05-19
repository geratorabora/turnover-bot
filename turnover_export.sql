with weeks as (
    select
        period::date as week_dt,
        dense_rank() over (order by period::date) as week_num
    from public.raw_turnover_stock
    group by period::date
),
base as (
    select
        r.period::date as week_dt,
        w.week_num,
        trim(r.pg) as pg,
        trim(r.segment) as segment,
        sum(r.curr_stock_cost) as stock_cost,
        sum(r.sales_cost) as sales_cost,
        sum(r.av_stock_cost) as av_stock_cost,
        sum(r.curr_stock_cost) filter (where r.turns_rub < 2 or r.turns_rub is null) as slow_stock_lt2,
        sum(r.curr_stock_cost) filter (where r.nonliq is true) as nonliq_stock
    from public.raw_turnover_stock r
    join weeks w on w.week_dt = r.period::date
    group by 1, 2, 3, 4
),
seg_rows as (
    select
        pg,
        segment,
        2 as lvl,
        ('   ' || segment) as pg_segment,
        week_dt,
        week_num,
        stock_cost,
        sales_cost,
        av_stock_cost,
        round(sales_cost / nullif(av_stock_cost, 0), 2) as turns_rub,
        slow_stock_lt2,
        nonliq_stock
    from base
    where segment is not null and segment <> ''
),
pg_rows as (
    select
        pg,
        null::text as segment,
        1 as lvl,
        pg as pg_segment,
        week_dt,
        week_num,
        sum(stock_cost) as stock_cost,
        sum(sales_cost) as sales_cost,
        sum(av_stock_cost) as av_stock_cost,
        round(sum(sales_cost) / nullif(sum(av_stock_cost), 0), 2) as turns_rub,
        sum(slow_stock_lt2) as slow_stock_lt2,
        sum(nonliq_stock) as nonliq_stock
    from base
    group by pg, week_dt, week_num
)
select
    pg,
    segment,
    lvl,
    pg_segment,
    week_dt,
    week_num,
    stock_cost,
    sales_cost,
    av_stock_cost,
    turns_rub,
    slow_stock_lt2,
    nonliq_stock
from pg_rows
union all
select
    pg,
    segment,
    lvl,
    pg_segment,
    week_dt,
    week_num,
    stock_cost,
    sales_cost,
    av_stock_cost,
    turns_rub,
    slow_stock_lt2,
    nonliq_stock
from seg_rows
order by
    week_num,
    pg,
    lvl,
    pg_segment;
