{{ config(tags=["staging"]) }}

select
  company_id,
  classifier_version,
  classified_at,
  is_built_world,
  theme,
  main_category,
  subcategory
from {{ source("investor_profiling", "silver_company_classification") }}
