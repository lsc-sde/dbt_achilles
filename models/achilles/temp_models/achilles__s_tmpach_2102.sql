-- 2102	Number of persons by device by  start month, by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    de.device_concept_id as stratum_1,
    YEAR(de.device_exposure_start_date) * 100
    + MONTH(de.device_exposure_start_date) as stratum_2,
    COUNT(distinct de.person_id) as count_value
  from
    {{ source("omop", "device_exposure" ) }} as de
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      de.person_id = op.person_id
      and
      de.device_exposure_start_date >= op.observation_period_start_date
      and
      de.device_exposure_start_date <= op.observation_period_end_date
  group by
    de.device_concept_id,
    YEAR(de.device_exposure_start_date) * 100
    + MONTH(de.device_exposure_start_date)
)

select
  2102 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
