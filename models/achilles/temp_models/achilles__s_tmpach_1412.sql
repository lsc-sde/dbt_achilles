-- 1412	Number of persons by payer plan period end month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1412 as analysis_id,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  DATEFROMPARTS(
    YEAR(payer_plan_period_start_date), MONTH(payer_plan_period_start_date), 1
  ) as stratum_1,
  COUNT_BIG(distinct p1.PERSON_ID) as count_value
from
  {{ ref(  var("achilles_source_schema") + "__person" ) }} as p1
inner join {{ ref(  var("achilles_source_schema") + "__payer_plan_period" ) }} as ppp1
  on p1.person_id = ppp1.person_id
group by
  DATEFROMPARTS(YEAR(payer_plan_period_start_date), MONTH(payer_plan_period_start_date), 1)
