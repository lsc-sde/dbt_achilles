--HINT DISTRIBUTE_ON_KEY(gender_concept_id)
with overallStats (
  gender_concept_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    gender_concept_id,
    cast(avg(1.0 * count_value) as FLOAT) as avg_value,
    cast(stddev(count_value) as FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from {{ ref( "achilles__rawData_106" ) }}
  group by gender_concept_id
),
statsView (gender_concept_id, count_value, total, rn) as (
  select
    gender_concept_id,
    count_value,
    count(*) as total,
    row_number() over (order by count_value) as rn
  from {{ ref( "achilles__rawData_106" ) }}
  group by gender_concept_id, count_value
),
priorStats (gender_concept_id, count_value, total, accumulated) as (
  select
    s.gender_concept_id,
    s.count_value,
    s.total,
    sum(p.total) as accumulated
  from statsView as s
  inner join
    statsView as p
    on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
  group by s.gender_concept_id, s.count_value, s.total, s.rn
)
select
  106 as analysis_id,
  cast(o.gender_concept_id as VARCHAR(255)) as gender_concept_id,
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
inner join overallStats as o on p.gender_concept_id = o.gender_concept_id
group by
  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
