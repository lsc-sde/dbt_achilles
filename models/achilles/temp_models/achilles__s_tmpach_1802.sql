-- 1802	Number of persons by measurement occurrence start month, by measurement_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    m.measurement_concept_id as stratum_1,
    YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date) as stratum_2,
    COUNT(distinct m.person_id) as count_value
  from
    {{ source("omop", "measurement" ) }} as m
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      m.person_id = op.person_id
      and
      m.measurement_date >= op.observation_period_start_date
      and
      m.measurement_date <= op.observation_period_end_date
  group by
    m.measurement_concept_id,
    YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date)
)

select
  1802 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
