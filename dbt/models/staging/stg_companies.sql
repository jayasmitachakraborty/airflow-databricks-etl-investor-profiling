{{ config(tags=["staging"]) }}

select
  company_id,
  canonical_name,
  canonical_website,
  homepage_text,
  about_text,
  normalized_domain,
  first_seen_at,
  last_seen_at
from {{ source("investor_profiling", "silver_companies") }}
