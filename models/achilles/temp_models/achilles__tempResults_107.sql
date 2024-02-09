-- 107	Length of observation (days) of first observation period by age decile
--HINT DISTRIBUTE_ON_KEY(age_decile)
with rawData (age_decile, count_value) as (
  select
    floor((year(op.OBSERVATION_PERIOD_START_DATE) - p.YEAR_OF_BIRTH) / 10)
      as age_decile,
    datediff( op.observation_period_end_date,op.observation_period_start_date
    ) as count_value
  from
    (
      select
        person_id,
        op.observation_period_start_date,
        op.observation_period_end_date,
        row_number() over (
          partition by op.person_id
          order by op.observation_period_start_date asc
        ) as rn
      from {{ source("omop", "observation_period" ) }} as op
    ) as op
  inner join {{ source("omop", "person" ) }} as p on op.person_id = p.person_id
  where op.rn = 1
),
overallStats (
  age_decile, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    age_decile,
    cast(avg(1.0 * count_value) as FLOAT) as avg_value,
    cast(stddev(count_value) as FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by age_decile
),
statsView (age_decile, count_value, total, rn) as (
  select
    age_decile,
    count_value,
    count(*) as total,
    row_number() over (order by count_value) as rn
  from rawData
  group by age_decile, count_value
),
priorStats (age_decile, count_value, total, accumulated) as (
  select
    s.age_decile,
    s.count_value,
    s.total,
    sum(p.total) as accumulated
  from statsView as s
  inner join statsView as p on s.age_decile = p.age_decile and p.rn <= s.rn
  group by s.age_decile, s.count_value, s.total, s.rn
)
select
  107 as analysis_id,
  cast(o.age_decile as VARCHAR(255)) as age_decile,
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
inner join overallStats as o on p.age_decile = o.age_decile
group by
  o.age_decile, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
