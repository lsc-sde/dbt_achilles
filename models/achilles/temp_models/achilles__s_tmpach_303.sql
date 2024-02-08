-- 303	Number of provider records, by specialty_concept_id, visit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  303 as analysis_id,
  cast(p.specialty_concept_id as varchar(255)) as stratum_1,
  cast(vo.visit_concept_id as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count(*) as count_value
from {{ source("omop", "provider" ) }} as p
inner join {{ source("omop", "visit_occurrence" ) }} as vo
  on vo.provider_id = p.provider_id
group by p.specialty_concept_id, visit_concept_id
