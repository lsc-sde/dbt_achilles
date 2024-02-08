select distinct person_id from {{ source("omop", "drug_exposure" ) }}
