
select * from {{ ref( 'FIELD_cdmDatatype_0000' ) }}
union all
select * from {{ ref( 'FIELD_cdmDatatype_0001' ) }}
union all
select * from {{ ref( 'FIELD_cdmDatatype_0002' ) }}
union all
select * from {{ ref( 'FIELD_cdmDatatype_0003' ) }}
