-- 106	Length of observation (days) of first observation period by gender
--HINT DISTRIBUTE_ON_KEY(gender_concept_id)
select
  p.gender_concept_id,
  op.count_value
from
  (
    select
      person_id,
      DATEDIFF(
        dd, op.observation_period_start_date, op.observation_period_end_date
      ) as count_value,
      row_number() over (
        partition by op.person_id order by op.observation_period_start_date asc
      ) as rn
    from {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} as op
  ) as op
inner join {{ ref(  var("achilles_source_schema") + "__person" ) }} as p on op.person_id = p.person_id
where op.rn = 1
