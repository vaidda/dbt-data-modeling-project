with source as (
    select * from "postgres"."public"."raw_crypto_prices"
),

renamed as (
    select
        coin_id,
        timestamp,
        price_usd
    from source
)

select * from renamed