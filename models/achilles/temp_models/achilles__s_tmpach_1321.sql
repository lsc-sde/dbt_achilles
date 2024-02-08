-- 1321	Number of persons by visit start year
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    YEAR(vd.visit_detail_start_date) as stratum_1,
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
    YEAR(vd.visit_detail_start_date)
)

select
  1321 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
