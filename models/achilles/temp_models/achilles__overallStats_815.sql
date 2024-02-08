--HINT DISTRIBUTE_ON_KEY(stratum1_id)
select
  o.subject_id as stratum1_id,
  o.unit_concept_id as stratum2_id,
  CAST(AVG(1.0 * o.count_value) as FLOAT) as avg_value,
  CAST(STDDEV(o.count_value) as FLOAT) as stdev_value,
  MIN(o.count_value) as min_value,
  MAX(o.count_value) as max_value,
  COUNT(*) as total
from (
  select
    o.observation_concept_id as subject_id,
    o.unit_concept_id,
    CAST(o.value_as_number as FLOAT) as count_value
  from
    {{ source("omop", "observation" ) }} as o
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      o.person_id = op.person_id
      and
      o.observation_date >= op.observation_period_start_date
      and
      o.observation_date <= op.observation_period_end_date
  where
    o.unit_concept_id is not NULL
    and
    o.value_as_number is not NULL
) as o
group by
  o.subject_id,
  o.unit_concept_id
