select
  id,
  coin_id,
  start_date,
  end_date,
  rows_inserted,
  run_timestamp::timestamp as run_ts
from {{ source('public', 'ingestion_runs') }}
