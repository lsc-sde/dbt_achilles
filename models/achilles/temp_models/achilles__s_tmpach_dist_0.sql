--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  0 as analysis_id,
  cast('' as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  cast(null as FLOAT) as min_value,
  cast(null as FLOAT) as max_value,
  cast(null as FLOAT) as avg_value,
  cast(null as FLOAT) as stdev_value,
  cast(null as FLOAT) as median_value,
  cast(null as FLOAT) as p10_value,
  cast(null as FLOAT) as p25_value,
  cast(null as FLOAT) as p75_value,
  cast(null as FLOAT) as p90_value,
  count(distinct person_id) as count_value
from {{ source("omop", "person" ) }}
