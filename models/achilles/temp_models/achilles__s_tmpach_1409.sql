--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1409 as analysis_id,
  cast(t1.obs_year as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  COUNT_BIG(distinct p1.PERSON_ID) as count_value
from
  {{ ref(  var("achilles_source_schema") + "__person" ) }} as p1
inner join
  {{ ref(  var("achilles_source_schema") + "__payer_plan_period" ) }} as ppp1
  on p1.person_id = ppp1.person_id
,
  {{ ref ("achilles__temp_dates_1409") }} as t1
where
  YEAR(ppp1.payer_plan_period_START_DATE) <= t1.obs_year
  and YEAR(ppp1.payer_plan_period_END_DATE) >= t1.obs_year
group by t1.obs_year
