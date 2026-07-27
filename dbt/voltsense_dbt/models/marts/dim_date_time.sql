{{ config(materialized='table') }}

with time_slots as (

    select
        dateadd(minute, seq * 15, '2026-07-01'::timestamp) as date_time_15min
    from (
        select seq4() as seq
        from table(generator(rowcount => 2880))
    )

)

select
    date_time_15min,
    date_time_15min::date as calendar_date,
    hour(date_time_15min) as hour_of_day,
    minute(date_time_15min) as minute_of_hour,
    dayofweek(date_time_15min) as day_of_week,
    dayname(date_time_15min) as day_name,
    weekofyear(date_time_15min) as week_of_year,
    month(date_time_15min) as month_num,
    year(date_time_15min) as year_num,
    case when dayofweek(date_time_15min) in (0, 6) then true else false end as is_weekend,
    case
        when hour(date_time_15min) between 6 and 9 then 'morning_commute'
        when hour(date_time_15min) between 10 and 15 then 'midday'
        when hour(date_time_15min) between 16 and 19 then 'evening_peak'
        when hour(date_time_15min) between 20 and 22 then 'evening'
        else 'overnight'
    end as time_period
from time_slots