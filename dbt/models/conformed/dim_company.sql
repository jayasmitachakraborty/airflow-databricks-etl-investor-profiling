{{ config(tags=["conformed"]) }}

select
  company_id,
  canonical_name,
  canonical_website,
  normalized_domain,
  first_seen_at,
  last_seen_at
from {{ ref("stg_companies") }}
