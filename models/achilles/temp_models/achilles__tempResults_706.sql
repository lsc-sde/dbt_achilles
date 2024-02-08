--HINT DISTRIBUTE_ON_KEY(stratum1_id)
with overallStats (
  stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    cast(avg(1.0 * count_value) as FLOAT) as avg_value,
    cast(stddev(count_value) as FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from {{ ref( "achilles__rawData_706" ) }}
  group by subject_id, gender_concept_id
),

statsView (stratum1_id, stratum2_id, count_value, total, rn) as (
  select
    subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    count_value,
    count(*) as total,
    row_number() over (
      partition by subject_id, gender_concept_id order by count_value
    ) as rn
  from {{ ref( "achilles__rawData_706" ) }}
  group by subject_id, gender_concept_id, count_value
),

priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as (
  select
    s.stratum1_id,
    s.stratum2_id,
    s.count_value,
    s.total,
    sum(p.total) as accumulated
  from statsView as s
  inner join
    statsView as p
    on
      s.stratum1_id = p.stratum1_id
      and s.stratum2_id = p.stratum2_id
      and s.rn >= p.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)

select
  706 as analysis_id,
  cast(o.stratum1_id as VARCHAR(255)) as stratum1_id,
  cast(o.stratum2_id as VARCHAR(255)) as stratum2_id,
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
inner join
  overallStats as o
  on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
group by
  o.stratum1_id,
  o.stratum2_id,
  o.total,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value
