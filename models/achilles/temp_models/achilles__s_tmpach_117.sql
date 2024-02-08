-- 117	Number of persons with at least one day of observation in each month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
-- generating date key sequences in a cross-dialect compatible fashion
with century as (
  select '19' as num
  union
  select '20' as num
),

tens as (
  select '0' as num
  union
  select '1' as num
  union
  select '2' as num
  union
  select '3' as num
  union
  select '4' as num
  union
  select '5' as num
  union
  select '6' as num
  union
  select '7' as num
  union
  select '8' as num
  union
  select '9' as num
),

ones as (
  select '0' as num
  union
  select '1' as num
  union
  select '2' as num
  union
  select '3' as num
  union
  select '4' as num
  union
  select '5' as num
  union
  select '6' as num
  union
  select '7' as num
  union
  select '8' as num
  union
  select '9' as num
),

months as (
  select '01' as num
  union
  select '02' as num
  union
  select '03' as num
  union
  select '04' as num
  union
  select '05' as num
  union
  select '06' as num
  union
  select '07' as num
  union
  select '08' as num
  union
  select '09' as num
  union
  select '10' as num
  union
  select '11' as num
  union
  select '12' as num
),

date_keys as (
  select cast(
    concat(century.num, tens.num, ones.num, months.num) as int
  ) as obs_month
  from century
  cross join tens
  cross join ones
  cross join months
)

select
  117 as analysis_id,
  cast(t1.obs_month as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  coalesce(count(distinct op1.PERSON_ID), 0) as count_value
from date_keys as t1
left join
  (
    select
      t2.obs_month,
      op2.*
    from {{ source("omop", "observation_period" ) }} as op2, date_keys as t2
    where
      year(op2.observation_period_start_date) * 100
      + month(op2.observation_period_start_date)
      <= t2.obs_month
      and year(op2.observation_period_end_date) * 100
      + month(op2.observation_period_end_date)
      >= t2.obs_month
  ) as op1 on t1.obs_month = op1.obs_month
group by t1.obs_month
having coalesce(count(distinct op1.PERSON_ID), 0) > 0
