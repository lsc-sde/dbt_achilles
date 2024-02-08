select
  1900 as analysis_id,
  cast('dummy' as varchar(255)) as stratum_1,
  cast('dummy' as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  0 as count_value


{# -- 1900	completeness report

--HINT DISTRIBUTE_ON_KEY(stratum_1)

select
  1900 as analysis_id,
  cast(table_name as varchar(255)) as stratum_1,
  cast(column_name as varchar(255)) as stratum_2,
  source_value as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  cnt as count_value

from (
  select
    'measurement' as table_name,
    'measurement_source_value' as column_name,
    measurement_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "measurement" ) }}
  where measurement_concept_id = 0
  group by measurement_source_value
  union
  select
    'measurement' as table_name,
    'unit_source_value' as column_name,
    unit_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "measurement" ) }}
  where unit_concept_id = 0
  group by unit_source_value
  union
  select
    'procedure_occurrence' as table_name,
    'procedure_source_value' as column_name,
    procedure_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "procedure_occurrence" ) }}
  where procedure_concept_id = 0
  group by procedure_source_value
  union
  select
    'procedure_occurrence' as table_name,
    'modifier_source_value' as column_name,
    modifier_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "procedure_occurrence" ) }}
  where modifier_concept_id = 0
  group by modifier_source_value
  union
  select
    'drug_exposure' as table_name,
    'drug_source_value' as column_name,
    drug_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "drug_exposure" ) }}
  where drug_concept_id = 0
  group by drug_source_value
  union
  select
    'drug_exposure' as table_name,
    'route_source_value' as column_name,
    route_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "drug_exposure" ) }}
  where route_concept_id = 0
  group by route_source_value
  union
  select
    'condition_occurrence' as table_name,
    'condition_source_value' as column_name,
    condition_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "condition_occurrence" ) }}
  where condition_concept_id = 0
  group by condition_source_value
  union
  select
    'condition_occurrence' as table_name,
    'condition_status_source_value' as column_name,
    condition_status_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "condition_occurrence" ) }}
  where condition_status_concept_id = 0
  group by condition_status_source_value
  union
  select
    'observation' as table_name,
    'observation_source_value' as column_name,
    observation_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "observation" ) }}
  where observation_concept_id = 0
  group by observation_source_value
  union
  select
    'observation' as table_name,
    'unit_source_value' as column_name,
    unit_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "observation" ) }}
  where unit_concept_id = 0
  group by unit_source_value
  union
  select
    'observation' as table_name,
    'qualifier_source_value' as column_name,
    qualifier_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "observation" ) }}
  where qualifier_concept_id = 0
  group by qualifier_source_value
  union
  select
    'payer_plan_period' as table_name,
    'payer_source_value' as column_name,
    payer_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "payer_plan_period" ) }}
  where payer_concept_id = 0
  group by payer_source_value
  union
  select
    'payer_plan_period' as table_name,
    'plan_source_value' as column_name,
    plan_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "payer_plan_period" ) }}
  where plan_concept_id = 0
  group by plan_source_value
  union
  select
    'payer_plan_period' as table_name,
    'sponsor_source_value' as column_name,
    sponsor_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "payer_plan_period" ) }}
  where sponsor_concept_id = 0
  group by sponsor_source_value
  union
  select
    'payer_plan_period' as table_name,
    'stop_reason_source_value' as column_name,
    stop_reason_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "payer_plan_period" ) }}
  where stop_reason_concept_id = 0
  group by stop_reason_source_value
  union
  select
    'provider' as table_name,
    'specialty_source_value' as column_name,
    specialty_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "provider" ) }}
  where specialty_concept_id = 0
  group by specialty_source_value
  union
  select
    'provider' as table_name,
    'gender_source_value' as column_name,
    gender_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "provider" ) }}
  where gender_concept_id = 0
  group by gender_source_value
  union
  select
    'person' as table_name,
    'gender_source_value' as column_name,
    gender_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "person" ) }}
  where gender_concept_id = 0
  group by gender_source_value
  union
  select
    'person' as table_name,
    'race_source_value' as column_name,
    race_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "person" ) }}
  where race_concept_id = 0
  group by race_source_value
  union
  select
    'person' as table_name,
    'ethnicity_source_value' as column_name,
    ethnicity_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "person" ) }}
  where ethnicity_concept_id = 0
  group by ethnicity_source_value
  union
  select
    'specimen' as table_name,
    'specimen_source_value' as column_name,
    specimen_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "specimen" ) }}
  where specimen_concept_id = 0
  group by specimen_source_value
  union
  select
    'specimen' as table_name,
    'unit_source_value' as column_name,
    unit_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "specimen" ) }}
  where unit_concept_id = 0
  group by unit_source_value
  union
  select
    'specimen' as table_name,
    'anatomic_site_source_value' as column_name,
    anatomic_site_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "specimen" ) }}
  where anatomic_site_concept_id = 0
  group by anatomic_site_source_value
  union
  select
    'specimen' as table_name,
    'disease_status_source_value' as column_name,
    disease_status_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "specimen" ) }}
  where disease_status_concept_id = 0
  group by disease_status_source_value

  union
  select
    'visit_occurrence' as table_name,
    'visit_source_value' as column_name,
    visit_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "visit_occurrence" ) }}
  where visit_concept_id = 0
  group by visit_source_value
  union

  select
    'visit_occurrence' as table_name,
    'admitted_from_source_value' as column_name,
    admitted_from_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "visit_occurrence" ) }}
  where admitted_from_concept_id = 0
  group by admitted_from_source_value
  union
  select
    'visit_occurrence' as table_name,
    'discharged_to_source_value' as column_name,
    discharged_to_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "visit_occurrence" ) }}
  where discharged_to_concept_id = 0
  group by discharged_to_source_value

  union
  select
    'device_exposure' as table_name,
    'device_source_value' as column_name,
    device_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "device_exposure" ) }}
  where device_concept_id = 0
  group by device_source_value
  union
  select
    'death' as table_name,
    'cause_source_value' as column_name,
    cause_source_value as source_value,
    count(*) as cnt
  from {{ source("omop", "death" ) }}
  where cause_concept_id = 0
  group by cause_source_value
) as a
where cnt >= 1 #}
