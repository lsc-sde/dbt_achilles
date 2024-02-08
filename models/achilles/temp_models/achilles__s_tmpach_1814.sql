-- 1814	Number of measurement records with no value (numeric or concept)
select
  1814 as analysis_id,
  CAST(NULL as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(m.person_id) as count_value
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
where
  m.value_as_number is NULL
  and
  (m.value_as_concept_id is NULL or m.value_as_concept_id = 0)
