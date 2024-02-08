-- 402	Number of persons by condition occurrence start month, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    co.condition_concept_id as stratum_1,
    YEAR(co.condition_start_date) * 100
    + MONTH(co.condition_start_date) as stratum_2,
    COUNT(distinct co.person_id) as count_value
  from
    {{ source("omop", "condition_occurrence" ) }} as co
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      co.person_id = op.person_id
      and
      co.condition_start_date >= op.observation_period_start_date
      and
      co.condition_start_date <= op.observation_period_end_date
  group by
    co.condition_concept_id,
    YEAR(co.condition_start_date) * 100 + MONTH(co.condition_start_date)
)

select
  402 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
