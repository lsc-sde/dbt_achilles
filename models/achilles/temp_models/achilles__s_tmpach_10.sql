-- 10	Number of all persons by year of birth and by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  10 as analysis_id,
  cast(year_of_birth as VARCHAR(255)) as stratum_1,
  cast(gender_concept_id as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct person_id) as count_value
from {{ source("omop", "person" ) }}
group by YEAR_OF_BIRTH, gender_concept_id
