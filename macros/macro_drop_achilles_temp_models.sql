{% macro drop_achilles_temp_models() %}
  
  drop schema if exists {{ target.schema}}_temp cascade

{% endmacro %}
