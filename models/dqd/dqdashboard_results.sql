select * from {{ ref( 'CONCEPT_plausibleGender' ) }}
union all
select * from {{ ref( 'CONCEPT_plausibleUnitConceptIds' ) }}
union all
select * from {{ ref( 'FIELD_cdmDatatype' ) }}
union all
select * from {{ ref( 'FIELD_cdmField' ) }}
union all
select * from {{ ref( 'FIELD_fkClass' ) }}
union all
select * from {{ ref( 'FIELD_fkDomain' ) }}
union all
select * from {{ ref( 'FIELD_isForeignKey' ) }}
union all
select * from {{ ref( 'FIELD_isPrimaryKey' ) }}
union all
select * from {{ ref( 'FIELD_isRequired' ) }}
union all
select * from {{ ref( 'FIELD_isStandardValidConcept' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness' ) }}
union all
select * from {{ ref( 'FIELD_plausibleDuringLife' ) }}
union all
select * from {{ ref( 'FIELD_plausibleTemporalAfter' ) }}
union all
select * from {{ ref( 'FIELD_plausibleValueHigh' ) }}
union all
select * from {{ ref( 'FIELD_plausibleValueLow' ) }}
union all
select * from {{ ref( 'FIELD_sourceConceptRecordCompleteness' ) }}
union all
select * from {{ ref( 'FIELD_sourceValueCompleteness' ) }}
union all
select * from {{ ref( 'FIELD_standardConceptRecordCompleteness' ) }}
union all
select * from {{ ref( 'FIELD_withinVisitDates' ) }}
union all
select * from {{ ref( 'TABLE_cdmTable' ) }}
union all
select * from {{ ref( 'TABLE_measureConditionEraCompleteness' ) }}
union all
select * from {{ ref( 'TABLE_measurePersonCompleteness' ) }}
