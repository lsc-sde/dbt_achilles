-- 720	Number of drug exposure records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    YEAR(de.drug_exposure_start_date) * 100
    + MONTH(de.drug_exposure_start_date) as stratum_1,
    COUNT(de.person_id) as count_value
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
  group by
    YEAR(de.drug_exposure_start_date) * 100 + MONTH(de.drug_exposure_start_date)
)

select
  720 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
