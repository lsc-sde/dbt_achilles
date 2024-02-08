-- 301	Number of providers by specialty concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  301 as analysis_id,
  cast(specialty_concept_id as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct provider_id) as count_value
from {{ source("omop", "provider" ) }}
group by specialty_CONCEPT_ID
