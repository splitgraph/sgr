{% macro splitgraph__get_relations () -%}
  {{ return(dbt.postgres__get_relations()) }}
{% endmacro %}
