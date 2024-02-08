select distinct person_id from {{ ref(  var("achilles_source_schema") + "__measurement" ) }}
