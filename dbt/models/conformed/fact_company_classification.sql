{{ config(tags=["conformed"]) }}

select
  c.company_id,
  cl.theme,
  cl.main_category,
  cl.subcategory,
  cl.classifier_version,
  cl.confidence as classification_confidence,
  cl.explanation,
  cl.classified_at
from {{ ref("stg_companies") }} c
inner join {{ ref("stg_company_classification") }} cl
  on c.company_id = cl.company_id
