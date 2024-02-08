-- 501	Number of records of death, by cause_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  501 as analysis_id,
  CAST(d.cause_concept_id as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(d.person_id) as count_value
from
  {{ source("omop", "death" ) }} as d
inner join
  {{ source("omop", "observation_period" ) }} as op
  on
    d.person_id = op.person_id
    and
    d.death_date >= op.observation_period_start_date
    and
    d.death_date <= op.observation_period_end_date
group by
  d.cause_concept_id
