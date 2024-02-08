-- 2002	patients with at least 1 Mes and 1 Dx and 1 Rx
select
  2002 as analysis_id,
  CAST(NULL as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  CAST(e.cnt as BIGINT) as count_value
from (
  select COUNT(*) as cnt
  from (
    select distinct person_id
    from (
      select m.person_id
      from
        {{ source("omop", "measurement" ) }} as m
      inner join
        {{ source("omop", "observation_period" ) }} as op
        on
          m.person_id = op.person_id
          and
          m.measurement_date >= op.observation_period_start_date
          and
          m.measurement_date <= op.observation_period_end_date
    ) as a
    intersect
    select distinct person_id
    from (
      select co.person_id
      from
        {{ source("omop", "condition_occurrence" ) }} as co
      inner join
        {{ source("omop", "observation_period" ) }} as op
        on
          co.person_id = op.person_id
          and
          co.condition_start_date >= op.observation_period_start_date
          and
          co.condition_start_date <= op.observation_period_end_date
    ) as b
    intersect
    select distinct person_id
    from (
      select de.person_id
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
    ) as c
  ) as d
) as e
