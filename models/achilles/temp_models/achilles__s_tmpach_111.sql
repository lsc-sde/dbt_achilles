-- 111	Number of persons by observation period start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    YEAR(op1.observation_period_start_date) * 100
    + MONTH(op1.OBSERVATION_PERIOD_START_DATE) as stratum_1,
    COUNT(distinct op1.PERSON_ID) as count_value
  from
    {{ source("omop", "observation_period" ) }} as op1
  group by
    YEAR(op1.observation_period_start_date) * 100
    + MONTH(op1.OBSERVATION_PERIOD_START_DATE)
)

select
  111 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(null as VARCHAR(255)) as stratum_2,
  CAST(null as VARCHAR(255)) as stratum_3,
  CAST(null as VARCHAR(255)) as stratum_4,
  CAST(null as VARCHAR(255)) as stratum_5
from rawData
