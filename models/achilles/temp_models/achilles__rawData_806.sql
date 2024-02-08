-- 806	Distribution of age by observation_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
select
  o.observation_concept_id as subject_id,
  p.gender_concept_id,
  o.observation_start_year - p.year_of_birth as count_value
from
  {{ source("omop", "person" ) }} as p
inner join (
  select
    o.person_id,
    o.observation_concept_id,
    MIN(YEAR(o.observation_date)) as observation_start_year
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
  group by
    o.person_id,
    o.observation_concept_id
) as o
  on
    p.person_id = o.person_id
