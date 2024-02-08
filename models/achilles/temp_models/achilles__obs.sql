select distinct person_id from {{ source("omop", "observation" ) }}
