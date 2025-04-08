with base as (
    select * from "crypto"."public"."int_crypto_metrics"
),

recent_data as (
    select *
    from base
    where date >= current_date - interval '30 days'
),

avg_by_coin as (
    select
        coin_id,
        round(avg(rolling_avg_7d)::numeric, 2) as avg_rolling_price
    from recent_data
    group by coin_id
)

select *
from avg_by_coin
order by avg_rolling_price desc
limit 5