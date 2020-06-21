
{% macro splitgraph__get_catalog(information_schema, schemas) %}
  {{ return(postgres__get_catalog(information_schema, schemas)) }}
{% endmacro %}
