-- 213	Distribution of length of stay by visit_concept_id
-- restrict to visits inside observation period
--HINT DISTRIBUTE_ON_KEY(stratum_id)
with rawData (stratum_id, count_value) as (
  select
    visit_concept_id as stratum_id,
    datediff( visit_end_date, visit_start_date) as count_value
  from {{ source("omop", "visit_occurrence" ) }} as vo inner join
    {{ source("omop", "observation_period" ) }} as op
    on vo.person_id = op.person_id
  -- only include events that occur during observation period
  where
    vo.visit_start_date >= op.observation_period_start_date
    and coalesce(vo.visit_end_date, vo.visit_start_date) <= op.observation_period_end_date
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
  213 as analysis_id,
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
