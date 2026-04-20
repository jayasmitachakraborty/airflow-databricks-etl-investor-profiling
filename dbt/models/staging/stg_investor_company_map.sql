{{ config(tags=["staging"]) }}

select
  investor_id,
  company_id,
  source_portfolio_url,
  extraction_run_id,
  evidence_text,
  confidence,
  is_active,
  discovered_at
from {{ source("investor_profiling", "silver_investor_company_map") }}
where is_active = true
