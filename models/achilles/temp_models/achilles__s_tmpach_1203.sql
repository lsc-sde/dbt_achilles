-- 1203	Number of visits by place of service discharge type
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1203 as analysis_id,
  CAST(vo.discharged_to_concept_id as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(*) as count_value
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
where
  vo.discharged_to_concept_id != 0
group by
  vo.discharged_to_concept_id
