{{ config(tags=["mart"]) }}

select
  i.investor_id,
  i.investor_name,
  count(distinct m.company_id) as portfolio_companies,
  max(m.discovered_at) as last_discovery_at,
  max(cl.classified_at) as last_classification_at
from {{ ref("stg_investors") }} i
left join {{ ref("stg_investor_company_map") }} m
  on i.investor_id = m.investor_id
left join {{ ref("stg_company_classification") }} cl
  on m.company_id = cl.company_id
group by i.investor_id, i.investor_name
