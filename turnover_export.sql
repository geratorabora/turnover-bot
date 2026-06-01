with weeks as (
    select
        period::date as week_dt,
        dense_rank() over (order by period::date) as week_num
    from public.raw_turnover_stock
    group by period::date
),
latest_report as (
    select max(period::date) as report_dt
    from public.raw_turnover_stock
),
statement_qty as (
    select
        report_dt,
        item_code,
        sum(stock_qty) as statement_qty
    from public.raw_stock_statement
    group by report_dt, item_code
),
cost_snapshot as (
    select
        report_dt,
        item_code,
        sum(stock_qty) as cost_qty,
        sum(stock_cost) as cost_total,
        case
            when nullif(sum(stock_qty), 0) is null then null
            else sum(stock_cost) / nullif(sum(stock_qty), 0)
        end as cost_unit
    from public.raw_stock_month_cost
    group by report_dt, item_code
),
item_base as (
    select
        r.period::date as week_dt,
        w.week_num,
        trim(r.pg) as pg,
        trim(r.segment) as segment,
        r.sales_cost,
        r.av_stock_cost,
        r.turns_rub,
        r.nonliq,
        case
            when r.period::date = lr.report_dt then coalesce(s.statement_qty, 0)
            else r.curr_stock_qty
        end as adjusted_stock_qty,
        case
            when r.period::date = lr.report_dt
                 and c.cost_qty is not null
                 and c.cost_unit is not null
                 and coalesce(s.statement_qty, 0) is not null
                then case
                    when c.cost_qty >= coalesce(s.statement_qty, 0)
                        then c.cost_unit * coalesce(s.statement_qty, 0)
                    else c.cost_total + (
                        greatest(coalesce(s.statement_qty, 0) - c.cost_qty, 0)
                        * coalesce(r.curr_stock_cost / nullif(r.curr_stock_qty, 0), 0)
                    )
                end
            when r.period::date = lr.report_dt
                 and s.statement_qty is not null
                 and nullif(r.curr_stock_qty, 0) is not null
                then (r.curr_stock_cost / nullif(r.curr_stock_qty, 0)) * s.statement_qty
            else r.curr_stock_cost
        end as adjusted_stock_cost
    from public.raw_turnover_stock r
    join weeks w on w.week_dt = r.period::date
    cross join latest_report lr
    left join statement_qty s
        on s.report_dt = r.period::date
       and s.item_code = r.item_code
    left join cost_snapshot c
        on c.report_dt = r.period::date
       and c.item_code = r.item_code
),
base as (
    select
        i.week_dt,
        i.week_num,
        i.pg,
        i.segment,
        sum(i.adjusted_stock_cost) as stock_cost,
        sum(i.sales_cost) as sales_cost,
        sum(i.av_stock_cost) as av_stock_cost,
        sum(i.adjusted_stock_cost) filter (where i.turns_rub < 2 or i.turns_rub is null) as slow_stock_lt2,
        sum(i.adjusted_stock_cost) filter (where i.nonliq is true) as nonliq_stock
    from item_base i
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
