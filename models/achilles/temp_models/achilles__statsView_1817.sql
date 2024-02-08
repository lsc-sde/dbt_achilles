--HINT DISTRIBUTE_ON_KEY(stratum1_id)
select
  m.subject_id as stratum1_id,
  m.unit_concept_id as stratum2_id,
  m.count_value,
  count(*) as total,
  row_number() over (
    partition by m.subject_id, m.unit_concept_id order by m.count_value
  ) as rn
from (
  select
    m.measurement_concept_id as subject_id,
    m.unit_concept_id,
    cast(m.range_high as FLOAT) as count_value
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
    m.unit_concept_id is not NULL
    and
    m.value_as_number is not NULL
    and
    m.range_low is not NULL
    and
    m.range_high is not NULL
) as m
group by
  m.subject_id,
  m.unit_concept_id,
  m.count_value
