-- 1413	Number of persons by number of payer plan periods
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1413 as analysis_id,
  cast(ppp1.num_periods as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct p1.PERSON_ID) as count_value
from
  {{ source("omop", "person" ) }} as p1
inner join
  (select
    person_id,
    count(payer_plan_period_start_date) as num_periods
  from {{ source("omop", "payer_plan_period" ) }} group by PERSON_ID) as ppp1
  on p1.person_id = ppp1.person_id
group by ppp1.num_periods
