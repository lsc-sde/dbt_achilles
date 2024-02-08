-- 716	Distribution of refills by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_id)
with rawData (stratum_id, count_value) as (
  select
    de.drug_concept_id as stratum_id,
    de.refills as count_value
  from
    {{ source("omop", "drug_exposure" ) }} as de
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      de.person_id = op.person_id
      and
      de.drug_exposure_start_date >= op.observation_period_start_date
      and
      de.drug_exposure_start_date <= op.observation_period_end_date
  where
    de.refills is not NULL
),

overallStats (
  stratum_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    stratum_id,
    CAST(AVG(1.0 * count_value) as FLOAT) as avg_value,
    CAST(STDDEV(count_value) as FLOAT) as stdev_value,
    MIN(count_value) as min_value,
    MAX(count_value) as max_value,
    COUNT(*) as total
  from rawData
  group by stratum_id
),

statsView (stratum_id, count_value, total, rn) as (
  select
    stratum_id,
    count_value,
    COUNT(*) as total,
    ROW_NUMBER() over (partition by stratum_id order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),

priorStats (stratum_id, count_value, total, accumulated) as (
  select
    s.stratum_id,
    s.count_value,
    s.total,
    SUM(p.total) as accumulated
  from statsView as s
  inner join statsView as p on s.stratum_id = p.stratum_id and s.rn >= p.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)

select
  716 as analysis_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(o.stratum_id as VARCHAR(255)) as stratum_id,
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
inner join overallStats as o on p.stratum_id = o.stratum_id
group by
  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
