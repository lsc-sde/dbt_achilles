-- 1821	Number of measurement records with no numeric value
select
  1821 as analysis_id,
  cast(null as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count(m.PERSON_ID) as count_value
from
  {{ source("omop", "measurement" ) }} as m
where m.value_as_number is null
