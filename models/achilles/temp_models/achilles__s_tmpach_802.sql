-- 802	Number of persons by observation occurrence start month, by observation_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    o.observation_concept_id as stratum_1,
    YEAR(o.observation_date) * 100 + MONTH(o.observation_date) as stratum_2,
    COUNT(distinct o.person_id) as count_value
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
    o.observation_concept_id,
    YEAR(o.observation_date) * 100 + MONTH(o.observation_date)
)

select
  802 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
