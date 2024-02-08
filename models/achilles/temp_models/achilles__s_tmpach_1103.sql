-- 1103	Number of care sites by location state
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1103 as analysis_id,
  cast(l1.state as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct care_site_id) as count_value
from {{ source("omop", "care_site" ) }} as cs1
inner join {{ source("omop", "location" ) }} as l1
  on cs1.location_id = l1.location_id
where
  cs1.location_id is not null
  and l1.state is not null
group by l1.state
