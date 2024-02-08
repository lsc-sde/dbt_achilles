-- 2001	patients with at least 1 Dx and 1 Proc
select
  2001 as analysis_id,
  CAST(NULL as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  CAST(d.cnt as BIGINT) as count_value
from (
  select COUNT(*) as cnt
  from (
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
    ) as a
    intersect
    select distinct person_id
    from (
      select po.person_id
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
    ) as b
  ) as c
) as d
