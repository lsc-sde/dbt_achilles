-- 1100	Number of persons by location 3-digit zip
--HINT DISTRIBUTE_ON_KEY(stratum_1)
with rawData as (
  select
    LEFT(l1.zip, 3) as stratum_1,
    COUNT(distinct person_id) as count_value
  from {{ source("omop", "person" ) }} as p1
  inner join {{ source("omop", "location" ) }} as l1
    on p1.location_id = l1.location_id
  where
    p1.location_id is not NULL
    and l1.zip is not NULL
  group by LEFT(l1.zip, 3)
)

select
  1100 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from rawData
