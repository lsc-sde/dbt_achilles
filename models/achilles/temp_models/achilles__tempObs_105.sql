-- 105	Length of observation (days) of first observation period
--HINT DISTRIBUTE_ON_KEY(count_value)
select
  count_value,
  rn
from
  (
    select
      datediff( op.observation_period_end_date,op.observation_period_start_date
      ) as count_value,
      ROW_NUMBER() over (
        partition by op.person_id order by op.observation_period_start_date asc
      ) as rn
    from {{ source("omop", "observation_period" ) }} as op
  ) as A
where rn = 1
