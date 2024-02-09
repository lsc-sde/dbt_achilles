-- 1406	Length of payer plan (days) of first payer plan period by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData (stratum1_id, count_value) as (
  select
    p1.gender_concept_id as stratum1_id,
    datediff( ppp1.payer_plan_period_end_date,ppp1.payer_plan_period_start_date
    ) as count_value
  from {{ source("omop", "person" ) }} as p1
  inner join
    (
      select
        person_id,
        payer_plan_period_START_DATE,
        payer_plan_period_END_DATE,
        ROW_NUMBER() over (
          partition by person_id order by payer_plan_period_start_date asc
        ) as rn1
      from {{ source("omop", "payer_plan_period" ) }}
    ) as ppp1
    on p1.PERSON_ID = ppp1.PERSON_ID
  where ppp1.rn1 = 1
),

overallStats (
  stratum1_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    stratum1_id,
    CAST(AVG(1.0 * count_value) as FLOAT) as avg_value,
    CAST(STDDEV(count_value) as FLOAT) as stdev_value,
    MIN(count_value) as min_value,
    MAX(count_value) as max_value,
    COUNT(*) as total
  from rawData
  group by stratum1_id
),

statsView (stratum1_id, count_value, total, rn) as (
  select
    stratum1_id,
    count_value,
    COUNT(*) as total,
    ROW_NUMBER() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),

priorStats (stratum1_id, count_value, total, accumulated) as (
  select
    s.stratum1_id,
    s.count_value,
    s.total,
    SUM(p.total) as accumulated
  from statsView as s
  inner join statsView as p on s.stratum1_id = p.stratum1_id and s.rn >= p.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)

select
  1406 as analysis_id,
  CAST(p.stratum1_id as VARCHAR(255)) as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  MIN(
    case
      when p.accumulated >= .50 * o.total then count_value else o.max_value
    end
  ) as median_value,
  MIN(
    case
      when p.accumulated >= .10 * o.total then count_value else o.max_value
    end
  ) as p10_value,
  MIN(
    case
      when p.accumulated >= .25 * o.total then count_value else o.max_value
    end
  ) as p25_value,
  MIN(
    case
      when p.accumulated >= .75 * o.total then count_value else o.max_value
    end
  ) as p75_value,
  MIN(
    case
      when p.accumulated >= .90 * o.total then count_value else o.max_value
    end
  ) as p90_value
from priorStats as p
inner join overallStats as o on p.stratum1_id = o.stratum1_id
group by
  p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
