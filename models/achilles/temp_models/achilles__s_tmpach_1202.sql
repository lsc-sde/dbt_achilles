-- 1202	Number of care sites by place of service
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1202 as analysis_id,
  cast(cs1.place_of_service_concept_id as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(cs1.care_site_id) as count_value
from {{ source("omop", "care_site" ) }} as cs1
where cs1.place_of_service_concept_id is not null
group by cs1.place_of_service_concept_id
