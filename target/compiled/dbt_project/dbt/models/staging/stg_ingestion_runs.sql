select
  id,
  coin_id,
  start_date,
  end_date,
  rows_inserted,
  run_timestamp::timestamp as run_ts
from "crypto"."public"."ingestion_runs"