-- 12	Number of persons by race and ethnicity
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  12 as analysis_id,
  cast(RACE_CONCEPT_ID as VARCHAR(255)) as stratum_1,
  cast(ETHNICITY_CONCEPT_ID as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct person_id) as count_value
from {{ source("omop", "person" ) }}
group by RACE_CONCEPT_ID, ETHNICITY_CONCEPT_ID
