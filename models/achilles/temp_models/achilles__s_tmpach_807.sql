-- 807	Number of observation occurrence records, by observation_concept_id and unit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  807 as analysis_id,
  CAST(o.observation_concept_id as VARCHAR(255)) as stratum_1,
  CAST(o.unit_concept_id as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(o.person_id) as count_value
from
  {{ source("omop", "observation" ) }} as o
inner join
  {{ source("omop", "observation_period" ) }} as op
  on
    o.person_id = op.person_id
    and
    o.observation_date >= op.observation_period_start_date
    and
    o.observation_date <= op.observation_period_end_date
group by
  o.observation_CONCEPT_ID,
  o.unit_concept_id
