-- 2191	Number of total persons that have at least x measurements
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  2191 as analysis_id,
  CAST(d.device_concept_id as VARCHAR(255)) as stratum_1,
  CAST(d.device_count as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  SUM(COUNT(d.person_id))
    over (partition by d.device_concept_id order by d.device_count desc)
  as count_value
from (
  select
    d.device_concept_id,
    d.person_id,
    COUNT(d.device_exposure_id) as device_count
  from
    {{ source("omop", "device_exposure" ) }} as d
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      d.person_id = op.person_id
      and
      d.device_exposure_start_date >= op.observation_period_start_date
      and
      d.device_exposure_start_date <= op.observation_period_end_date
  group by
    d.person_id,
    d.device_concept_id
) as d
group by
  d.device_concept_id,
  d.device_count
