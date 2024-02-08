-- 325	Number of provider records, by specialty_source_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  325 as analysis_id,
  cast(specialty_source_concept_id as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_big(*) as count_value
from {{ ref(  var("achilles_source_schema") + "__provider" ) }}
group by specialty_source_concept_id
