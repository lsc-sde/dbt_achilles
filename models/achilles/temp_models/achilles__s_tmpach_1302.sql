-- 1302	Number of persons by visit detail start month, by visit_detail_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    vd.visit_detail_concept_id as stratum_1,
    YEAR(vd.visit_detail_start_date) * 100
    + MONTH(vd.visit_detail_start_date) as stratum_2,
    COUNT(distinct vd.person_id) as count_value
  from
    {{ source("omop", "visit_detail" ) }} as vd
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      vd.person_id = op.person_id
      and
      vd.visit_detail_start_date >= op.observation_period_start_date
      and
      vd.visit_detail_start_date <= op.observation_period_end_date
  group by
    vd.visit_detail_concept_id,
    YEAR(vd.visit_detail_start_date) * 100 + MONTH(vd.visit_detail_start_date)
)

select
  1302 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
