-- 1803	Number of distinct measurement occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData (count_value) as (
  select COUNT_BIG(distinct m.measurement_concept_id) as count_value
  from
    {{ ref(  var("achilles_source_schema") + "__measurement" ) }} as m
    join
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} as op
    on
      m.person_id = op.person_id
      and
      m.measurement_date >= op.observation_period_start_date
      and
      m.measurement_date <= op.observation_period_end_date
  group by
    m.person_id
),
overallStats (avg_value, stdev_value, min_value, max_value, total) as (
  select
    cast(AVG(1.0 * count_value) as FLOAT) as avg_value,
    cast(STDEV(count_value) as FLOAT) as stdev_value,
    MIN(count_value) as min_value,
    MAX(count_value) as max_value,
    COUNT_BIG(*) as total
  from rawData
),
statsView (count_value, total, rn) as (
  select
    count_value,
    COUNT_BIG(*) as total,
    row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats (count_value, total, accumulated) as (
  select
    s.count_value,
    s.total,
    SUM(p.total) as accumulated
  from statsView as s
  inner join statsView as p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select
  1803 as analysis_id,
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
cross join overallStats as o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
