-- 2	Number of persons by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  2 as analysis_id,
  cast(gender_concept_id as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  COUNT_BIG(distinct person_id) as count_value
from {{ ref(  var("achilles_source_schema") + "__person" ) }}
group by GENDER_CONCEPT_ID
