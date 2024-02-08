-- 1002	Number of persons by condition occurrence start month, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    ce.condition_concept_id as stratum_1,
    YEAR(ce.condition_era_start_date) * 100
    + MONTH(ce.condition_era_start_date) as stratum_2,
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
    ce.condition_concept_id,
    YEAR(ce.condition_era_start_date) * 100 + MONTH(ce.condition_era_start_date)
)

select
  1002 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
