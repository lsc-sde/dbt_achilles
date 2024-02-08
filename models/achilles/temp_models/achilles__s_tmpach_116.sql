--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    t1.obs_year as stratum_1,
    p1.gender_concept_id as stratum_2,
    floor((t1.obs_year - p1.year_of_birth) / 10) as stratum_3,
    count(distinct p1.PERSON_ID) as count_value
  from
    {{ source("omop", "person" ) }} as p1
  inner join
    {{ source("omop", "observation_period" ) }} as op1
    on p1.person_id = op1.person_id,
    {{ ref ("achilles__temp_dates_116") }} as t1
  where
    year(op1.OBSERVATION_PERIOD_START_DATE) <= t1.obs_year
    and year(op1.OBSERVATION_PERIOD_END_DATE) >= t1.obs_year
  group by
    t1.obs_year,
    p1.gender_concept_id,
    floor((t1.obs_year - p1.year_of_birth) / 10)
)

select
  116 as analysis_id,
  count_value,
  cast(stratum_1 as VARCHAR(255)) as stratum_1,
  cast(stratum_2 as VARCHAR(255)) as stratum_2,
  cast(stratum_3 as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5
from rawData
