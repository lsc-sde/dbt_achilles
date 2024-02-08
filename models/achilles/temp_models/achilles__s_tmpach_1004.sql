-- 1004	Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    ce.condition_concept_id as stratum_1,
    p.gender_concept_id as stratum_3,
    YEAR(ce.condition_era_start_date) as stratum_2,
    FLOOR((YEAR(ce.condition_era_start_date) - p.year_of_birth) / 10)
    as stratum_4,
    COUNT(distinct p.person_id) as count_value
  from
    {{ source("omop", "person" ) }} as p
  inner join
    {{ source("omop", "condition_era" ) }} as ce
    on
      p.person_id = ce.person_id
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      ce.person_id = op.person_id
      and
      ce.condition_era_start_date >= op.observation_period_start_date
      and
      ce.condition_era_start_date <= op.observation_period_end_date
  group by
    ce.condition_concept_id,
    YEAR(ce.condition_era_start_date),
    p.gender_concept_id,
    FLOOR((YEAR(ce.condition_era_start_date) - p.year_of_birth) / 10)
)

select
  1004 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(stratum_3 as VARCHAR(255)) as stratum_3,
  CAST(stratum_4 as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
