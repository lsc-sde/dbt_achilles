-- 226	Number of records by domain by visit concept id
select
  226 as analysis_id,
  cast(v.visit_concept_id as VARCHAR(255)) as stratum_1,
  v.cdm_table as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  v.record_count as count_value
from (
  select
    'drug_exposure' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from {{ ref(  var("achilles_source_schema") + "__drug_exposure" ) }} as t
  left join
    {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'condition_occurrence' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from {{ ref(  var("achilles_source_schema") + "__condition_occurrence" ) }} as t
  left join
    {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'device_exposure' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from {{ ref(  var("achilles_source_schema") + "__device_exposure" ) }} as t
  left join
    {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'procedure_occurrence' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from {{ ref(  var("achilles_source_schema") + "__procedure_occurrence" ) }} as t
  left join
    {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'measurement' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from {{ ref(  var("achilles_source_schema") + "__measurement" ) }} as t
  left join
    {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'observation' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from {{ ref(  var("achilles_source_schema") + "__observation" ) }} as t
  left join
    {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
) as v
