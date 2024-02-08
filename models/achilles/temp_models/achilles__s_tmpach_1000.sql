-- 1000	Number of persons with at least one condition occurrence, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1000 as analysis_id,
  CAST(ce.condition_concept_id as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(distinct ce.person_id) as count_value
from
  {{ source("omop", "condition_era" ) }} as ce
inner join
  {{ source("omop", "observation_period" ) }} as op
  on
    ce.person_id = op.person_id
    and
    ce.condition_era_start_date >= op.observation_period_start_date
    and
    ce.condition_era_start_date <= op.observation_period_end_date
group by
  ce.condition_concept_id
