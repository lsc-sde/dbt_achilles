
select * from {{ ref( 'FIELD_isStandardValidConcept_0000' ) }}
union all
select * from {{ ref( 'FIELD_isStandardValidConcept_0001' ) }}
