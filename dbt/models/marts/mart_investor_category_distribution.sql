{{ config(tags=["mart"]) }}

select
  i.investor_id,
  i.investor_name,
  cl.main_category,
  cl.subcategory,
  count(distinct m.company_id) as company_count,
  avg(cl.confidence) as avg_classification_confidence
from {{ ref("stg_investors") }} i
inner join {{ ref("stg_investor_company_map") }} m
  on i.investor_id = m.investor_id
inner join {{ ref("stg_company_classification") }} cl
  on m.company_id = cl.company_id
group by i.investor_id, i.investor_name, cl.main_category, cl.subcategory
