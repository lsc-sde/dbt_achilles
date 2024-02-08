-- 2201	Number of note records, by note_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  2201 as analysis_id,
  cast(m.note_type_CONCEPT_ID as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  COUNT_BIG(m.PERSON_ID) as count_value
from
  {{ ref(  var("achilles_source_schema") + "__note" ) }} as m
group by m.note_type_CONCEPT_ID
