
  create view "crypto"."public"."int_crypto_metrics__dbt_tmp"
    
    
  as (
    with base as (
    select * from "crypto"."public_staging"."stg_crypto_prices"
),

with_changes as (
    select
        coin_id,
        timestamp::date as date,
        price_usd,
        lag(price_usd) over (partition by coin_id order by timestamp) as previous_price
    from base
),

with_pct_change as (
    select
        coin_id,
        date,
        price_usd,
        previous_price,
        round(
            case
                when previous_price is not null and previous_price != 0
                then 100.0 * (price_usd - previous_price) / previous_price
                else null
            end::numeric, 2
        ) as price_change_pct
    from with_changes
),

with_rolling as (
    select
        *,
        avg(price_usd) over (
            partition by coin_id
            order by date
            rows between 6 preceding and current row
        ) as raw_rolling_avg_7d
    from with_pct_change
)

select
    coin_id,
    date,
    price_usd,
    price_change_pct,
    round(raw_rolling_avg_7d::numeric, 2) as rolling_avg_7d
from with_rolling
  );