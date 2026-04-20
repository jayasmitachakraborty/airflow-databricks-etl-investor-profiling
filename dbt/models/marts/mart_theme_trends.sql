{{ config(tags=["mart"]) }}

select
  date_trunc('month', cl.classified_at) as activity_month,
  cl.theme,
  count(distinct cl.company_id) as companies_classified,
  avg(cl.confidence) as avg_confidence
from {{ ref("stg_company_classification") }} cl
group by activity_month, cl.theme
