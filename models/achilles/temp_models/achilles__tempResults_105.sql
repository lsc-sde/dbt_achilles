--HINT DISTRIBUTE_ON_KEY(count_value)
with overallStats (avg_value, stdev_value, min_value, max_value, total) as (
  select
    cast(avg(1.0 * count_value) as FLOAT) as avg_value,
    cast(stddev(count_value) as FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from {{ ref( "achilles__tempObs_105" ) }}
),

priorStats (count_value, total, accumulated) as (
  select
    s.count_value,
    s.total,
    sum(p.total) as accumulated
  from {{ ref( "achilles__statsView_105" ) }} as s
  inner join {{ ref('achilles__statsView_105') }} as p on s.rn >= p.rn
  group by s.count_value, s.total, s.rn
)

select
  105 as analysis_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value end)
  as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value end)
  as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value end)
  as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value end)
  as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value end)
  as p90_value
from priorStats as p
cross join overallStats as o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
