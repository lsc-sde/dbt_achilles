-- 110	Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  110 as analysis_id,
  CAST(t1.obs_month as VARCHAR(255)) as stratum_1,
  CAST(null as VARCHAR(255)) as stratum_2,
  CAST(null as VARCHAR(255)) as stratum_3,
  CAST(null as VARCHAR(255)) as stratum_4,
  CAST(null as VARCHAR(255)) as stratum_5,
  COUNT(distinct op1.PERSON_ID) as count_value
from
  {{ source("omop", "observation_period" ) }} as op1
inner join
  (
    select distinct
      YEAR(observation_period_start_date) * 100
      + MONTH(observation_period_start_date) as obs_month,
      MAKE_DATE(
        YEAR(observation_period_start_date),
        MONTH(observation_period_start_date),
        1
      )
      as obs_month_start,
      LAST_DAY(observation_period_start_date) as obs_month_end
    from {{ source("omop", "observation_period" ) }}
  ) as t1
  on
    op1.observation_period_start_date <= t1.obs_month_start
    and op1.observation_period_end_date >= t1.obs_month_end
group by t1.obs_month
