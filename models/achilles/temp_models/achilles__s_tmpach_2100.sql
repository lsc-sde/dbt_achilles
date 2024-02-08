-- 2100	Number of persons with at least one device exposure , by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  2100 as analysis_id,
  CAST(de.device_concept_id as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
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
  de.device_concept_id
