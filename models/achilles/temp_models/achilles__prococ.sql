select distinct person_id from {{ source("omop", "procedure_occurrence" ) }}
