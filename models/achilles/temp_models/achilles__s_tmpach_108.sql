-- 108	Number of persons by length of observation period, in 30d increments
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    floor(
      datediff( op1.observation_period_end_date,op1.observation_period_start_date
      )
      / 30
    ) as stratum_1,
    count(distinct p1.person_id) as count_value
  from {{ source("omop", "person" ) }} as p1
  inner join
    (
      select
        person_id,
        OBSERVATION_PERIOD_START_DATE,
        OBSERVATION_PERIOD_END_DATE,
        row_number() over (
          partition by person_id order by observation_period_start_date asc
        ) as rn1
      from {{ source("omop", "observation_period" ) }}
    ) as op1
    on p1.PERSON_ID = op1.PERSON_ID
  where op1.rn1 = 1
  group by
    floor(
      datediff( op1.observation_period_end_date,op1.observation_period_start_date
      )
      / 30
    )
)

select
  108 as analysis_id,
  count_value,
  cast(stratum_1 as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5
from rawData
