with base as (
    select * from {{ ref('int_crypto_metrics') }}
),

with_weekly as (
    select
        coin_id,
        date_trunc('week', date) as week_start,
        first_value(price_usd) over (partition by coin_id, date_trunc('week', date) order by date) as first_price,
        last_value(price_usd) over (partition by coin_id, date_trunc('week', date) order by date
            rows between unbounded preceding and unbounded following) as last_price
    from base
),

deduped as (
    select distinct coin_id, week_start, first_price, last_price,
        round(((last_price - first_price) / first_price * 100)::numeric, 2) as pct_gain
    from with_weekly
),

ranked as (
    select *,
        rank() over (partition by week_start order by pct_gain desc) as rank
    from deduped
)

select * from ranked
