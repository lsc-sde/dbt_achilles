-- 1407	Length of payer plan (days) of first payer plan period by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_id)
with rawData (stratum_id, count_value) as (
  select
    floor((year(ppp1.payer_plan_period_START_DATE) - p1.YEAR_OF_BIRTH) / 10)
      as stratum_id,
    datediff( ppp1.payer_plan_period_end_date,ppp1.payer_plan_period_start_date
    ) as count_value
  from {{ source("omop", "person" ) }} as p1
  inner join
    (
      select
        person_id,
        payer_plan_period_START_DATE,
        payer_plan_period_END_DATE,
        row_number() over (
          partition by person_id order by payer_plan_period_start_date asc
        ) as rn1
      from {{ source("omop", "payer_plan_period" ) }}
    ) as ppp1
    on p1.PERSON_ID = ppp1.PERSON_ID
  where ppp1.rn1 = 1
),
overallStats (
  stratum_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    stratum_id,
    cast(avg(1.0 * count_value) as FLOAT) as avg_value,
    cast(stddev(count_value) as FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum_id
),
statsView (stratum_id, count_value, total, rn) as (
  select
    stratum_id,
    count_value,
    count(*) as total,
    row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats (stratum_id, count_value, total, accumulated) as (
  select
    s.stratum_id,
    s.count_value,
    s.total,
    sum(p.total) as accumulated
  from statsView as s
  inner join statsView as p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select
  1407 as analysis_id,
  cast(o.stratum_id as VARCHAR(255)) as stratum_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(
    case
      when p.accumulated >= .50 * o.total then count_value else o.max_value
    end
  ) as median_value,
  min(
    case
      when p.accumulated >= .10 * o.total then count_value else o.max_value
    end
  ) as p10_value,
  min(
    case
      when p.accumulated >= .25 * o.total then count_value else o.max_value
    end
  ) as p25_value,
  min(
    case
      when p.accumulated >= .75 * o.total then count_value else o.max_value
    end
  ) as p75_value,
  min(
    case
      when p.accumulated >= .90 * o.total then count_value else o.max_value
    end
  ) as p90_value
from priorStats as p
inner join overallStats as o on p.stratum_id = o.stratum_id
group by
  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
