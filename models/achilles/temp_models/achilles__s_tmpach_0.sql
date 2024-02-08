-- 0	cdm name, version of Achilles and date when pre-computations were executed
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  0 as analysis_id,
  cast('' as VARCHAR(255)) as stratum_1,
  cast('1.7.2' as VARCHAR(255)) as stratum_2,
  cast(getdate() as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct person_id) as count_value
from {{ source("omop", "person" ) }}
