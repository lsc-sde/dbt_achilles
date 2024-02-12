
select * from {{ ref( 'FIELD_cdmField_0000' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0001' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0002' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0003' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0004' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0005' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0006' ) }}
union all
select * from {{ ref( 'FIELD_cdmField_0007' ) }}
