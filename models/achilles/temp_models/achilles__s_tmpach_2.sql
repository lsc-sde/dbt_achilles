-- 2	Number of persons by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  2 as analysis_id,
  cast(gender_concept_id as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct person_id) as count_value
from {{ source("omop", "person" ) }}
group by GENDER_CONCEPT_ID
