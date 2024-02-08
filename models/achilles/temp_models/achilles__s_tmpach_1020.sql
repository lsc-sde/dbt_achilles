-- 1020	Number of condition era records by condition era start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    YEAR(ce.condition_era_start_date) * 100
    + MONTH(ce.condition_era_start_date) as stratum_1,
    COUNT(ce.person_id) as count_value
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
    YEAR(ce.condition_era_start_date) * 100 + MONTH(ce.condition_era_start_date)
)

select
  1020 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
