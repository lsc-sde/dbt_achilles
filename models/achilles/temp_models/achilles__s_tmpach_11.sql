-- 11	Number of non-deceased persons by year of birth and by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  11 as analysis_id,
  cast(P.year_of_birth as VARCHAR(255)) as stratum_1,
  cast(P.gender_concept_id as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct P.person_id) as count_value
from {{ source("omop", "person" ) }} as P
where
  not exists
  (
    select 1
    from {{ source("omop", "death" ) }} as D
    where P.person_id = D.person_id
  )
group by P.YEAR_OF_BIRTH, P.gender_concept_id
