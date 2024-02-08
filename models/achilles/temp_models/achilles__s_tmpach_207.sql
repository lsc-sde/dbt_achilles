--207	Number of visit records with invalid person_id
select
  207 as analysis_id,
  cast(null as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count(vo1.PERSON_ID) as count_value
from
  {{ source("omop", "visit_occurrence" ) }} as vo1
left join {{ source("omop", "person" ) }} as p1
  on p1.person_id = vo1.person_id
where p1.person_id is null
