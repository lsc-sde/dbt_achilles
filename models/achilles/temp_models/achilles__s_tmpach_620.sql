-- 620	Number of procedure occurrence records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date) as stratum_1,
    COUNT(po.person_id) as count_value
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
    YEAR(po.procedure_date) * 100 + MONTH(po.procedure_date)
)

select
  620 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
