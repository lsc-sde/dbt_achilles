-- 1806	Distribution of age by measurement_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
select
  o.measurement_concept_id as subject_id,
  p.gender_concept_id,
  o.measurement_start_year - p.year_of_birth as count_value
from
  {{ source("omop", "person" ) }} as p
inner join (
  select
    m.person_id,
    m.measurement_concept_id,
    MIN(YEAR(m.measurement_date)) as measurement_start_year
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
    m.person_id,
    m.measurement_concept_id
) as o
  on
    p.person_id = o.person_id
