-- 602	Number of persons by procedure occurrence start month, by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    po.procedure_concept_id as stratum_1,
    YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date) as stratum_2,
    COUNT(distinct po.person_id) as count_value
  from
    {{ source("omop", "procedure_occurrence" ) }} as po
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      po.person_id = op.person_id
      and
      po.procedure_date >= op.observation_period_start_date
      and
      po.procedure_date <= op.observation_period_end_date
  group by
    po.procedure_concept_id,
    YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date)
)

select
  602 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
