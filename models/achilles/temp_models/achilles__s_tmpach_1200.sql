-- 1200	Number of persons by place of service
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1200 as analysis_id,
  cast(cs1.place_of_service_concept_id as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  COUNT_BIG(person_id) as count_value
from {{ ref(  var("achilles_source_schema") + "__person" ) }} as p1
inner join {{ ref(  var("achilles_source_schema") + "__care_site" ) }} as cs1
  on p1.care_site_id = cs1.care_site_id
where
  p1.care_site_id is not null
  and cs1.place_of_service_concept_id is not null
group by cs1.place_of_service_concept_id
