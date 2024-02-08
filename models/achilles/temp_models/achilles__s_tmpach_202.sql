-- 202	Number of persons by visit occurrence start month, by visit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    vo.visit_concept_id as stratum_1,
    YEAR(vo.visit_start_date) * 100 + MONTH(vo.visit_start_date) as stratum_2,
    COUNT(distinct vo.person_id) as count_value
  from
    {{ source("omop", "visit_occurrence" ) }} as vo
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      vo.person_id = op.person_id
      and
      vo.visit_start_date >= op.observation_period_start_date
      and
      vo.visit_start_date <= op.observation_period_end_date
  group by
    vo.visit_concept_id,
    YEAR(vo.visit_start_date) * 100 + MONTH(vo.visit_start_date)
)

select
  202 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
