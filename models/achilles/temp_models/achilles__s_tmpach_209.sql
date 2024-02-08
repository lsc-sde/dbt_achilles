-- 209	Number of visit records with invalid care_site_id
select
  209 as analysis_id,
  cast(null as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  COUNT_BIG(vo1.PERSON_ID) as count_value
from
  {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as vo1
left join {{ ref(  var("achilles_source_schema") + "__care_site" ) }} as cs1
  on vo1.care_site_id = cs1.care_site_id
where
  vo1.care_site_id is not null
  and cs1.care_site_id is null
