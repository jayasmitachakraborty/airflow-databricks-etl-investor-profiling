{{ config(tags=["mart"]) }}

select
  i.investor_id,
  i.investor_name,
  cl.theme,
  count(distinct m.company_id) as company_count
from {{ ref("stg_investors") }} i
inner join {{ ref("stg_investor_company_map") }} m
  on i.investor_id = m.investor_id
inner join {{ ref("stg_company_classification") }} cl
  on m.company_id = cl.company_id
where cl.is_built_world = true
  and cl.theme is not null
group by i.investor_id, i.investor_name, cl.theme
