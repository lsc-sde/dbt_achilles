-- 113	Number of persons by number of observation periods
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  113 as analysis_id,
  cast(op1.num_periods as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  COUNT_BIG(distinct op1.PERSON_ID) as count_value
from
  (
    select
      person_id,
      COUNT_BIG(OBSERVATION_period_start_date) as num_periods
    from {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} group by PERSON_ID
  ) as op1
group by op1.num_periods
