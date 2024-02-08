-- 706	Distribution of age by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
select
  de.drug_concept_id as subject_id,
  p.gender_concept_id,
  de.drug_start_year - p.year_of_birth as count_value
from
  {{ source("omop", "person" ) }} as p
inner join (
  select
    de.person_id,
    de.drug_concept_id,
    MIN(YEAR(de.drug_exposure_start_date)) as drug_start_year
  from
    {{ source("omop", "drug_exposure" ) }} as de
  inner join
    {{ source("omop", "observation_period" ) }} as op
    on
      de.person_id = op.person_id
      and
      de.drug_exposure_start_date >= op.observation_period_start_date
      and
      de.drug_exposure_start_date <= op.observation_period_end_date
  group by
    de.person_id,
    de.drug_concept_id
) as de
  on
    p.person_id = de.person_id
