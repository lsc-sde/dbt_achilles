-- 502	Number of persons by death month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    YEAR(d.death_date) * 100 + MONTH(d.death_date) as stratum_1,
    COUNT(distinct d.person_id) as count_value
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
    YEAR(d.death_date) * 100 + MONTH(d.death_date)
)

select
  502 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
