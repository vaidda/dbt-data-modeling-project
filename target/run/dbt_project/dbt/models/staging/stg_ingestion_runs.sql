
  create view "crypto"."public_staging"."stg_ingestion_runs__dbt_tmp"
    
    
  as (
    select
  id,
  coin_id,
  start_date,
  end_date,
  rows_inserted,
  run_timestamp::timestamp as run_ts
from "crypto"."public"."ingestion_runs"
  );