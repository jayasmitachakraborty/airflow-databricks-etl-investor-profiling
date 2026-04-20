{{ config(tags=["staging"]) }}

select
  company_id,
  theme,
  main_category,
  subcategory,
  classifier_version,
  confidence,
  explanation,
  classified_at
from {{ source("investor_profiling", "silver_company_classification") }}
