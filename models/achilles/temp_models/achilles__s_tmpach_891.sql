-- 891	Number of total persons that have at least x observations
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  891 as analysis_id,
  CAST(o.observation_concept_id as VARCHAR(255)) as stratum_1,
  CAST(o.obs_cnt as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  SUM(COUNT(o.person_id))
    over (partition by o.observation_concept_id order by o.obs_cnt desc)
  as count_value
from (
  select
    o.observation_concept_id,
    o.person_id,
    COUNT(o.observation_id) as obs_cnt
  from
    {{ source("omop", "observation" ) }} as o
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      o.person_id = op.person_id
      and
      o.observation_date >= op.observation_period_start_date
      and
      o.observation_date <= op.observation_period_end_date
  group by
    o.person_id,
    o.observation_concept_id
) as o
group by
  o.observation_concept_id,
  o.obs_cnt
