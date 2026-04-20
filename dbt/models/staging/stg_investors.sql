{{ config(tags=["staging"]) }}

select
  investor_id,
  investor_name,
  source_website,
  source_system,
  ingested_at
from {{ source("investor_profiling", "bronze_investors") }}
