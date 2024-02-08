-- 104	Distribution of age at first observation period by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData (gender_concept_id, age_value) as (
  select
    p.gender_concept_id,
    MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH as age_value
  from {{ source("omop", "person" ) }} as p
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on p.person_id = op.person_id
  group by p.person_id, p.gender_concept_id, p.year_of_birth
),
overallStats (
  gender_concept_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    gender_concept_id,
    cast(AVG(1.0 * age_value) as FLOAT) as avg_value,
    cast(stddev(age_value) as FLOAT) as stdev_value,
    MIN(age_value) as min_value,
    MAX(age_value) as max_value,
    count(*) as total
  from rawData
  group by gender_concept_id
),
ageStats (gender_concept_id, age_value, total, rn) as (
  select
    gender_concept_id,
    age_value,
    count(*) as total,
    row_number() over (order by age_value) as rn
  from rawData
  group by gender_concept_id, age_value
),
ageStatsPrior (gender_concept_id, age_value, total, accumulated) as (
  select
    s.gender_concept_id,
    s.age_value,
    s.total,
    SUM(p.total) as accumulated
  from ageStats as s
  inner join
    ageStats as p
    on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
  group by s.gender_concept_id, s.age_value, s.total, s.rn
)
select
  104 as analysis_id,
  cast(o.gender_concept_id as VARCHAR(255)) as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  MIN(case when p.accumulated >= .50 * o.total then age_value end)
    as median_value,
  MIN(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
  MIN(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
  MIN(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
  MIN(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
from ageStatsPrior as p
inner join overallStats as o on p.gender_concept_id = o.gender_concept_id
group by
  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
