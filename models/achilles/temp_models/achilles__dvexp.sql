select distinct person_id from {{ source("omop", "device_exposure" ) }}
