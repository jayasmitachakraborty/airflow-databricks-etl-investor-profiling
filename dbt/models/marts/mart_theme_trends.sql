{{ config(tags=["mart"]) }}

select
  date_trunc('month', cl.classified_at) as activity_month,
  cl.theme,
  count(distinct cl.company_id) as companies_classified
from {{ ref("stg_company_classification") }} cl
where cl.is_built_world = true
  and cl.theme is not null
group by activity_month, cl.theme
