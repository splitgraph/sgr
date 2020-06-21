
{% macro splitgraph__create_table_as(temporary, relation, sql) -%}
  {{ postgres__create_table_as(temporary, relation, sql) }}
{%- endmacro %}


{% macro splitgraph__create_schema(relation) -%}
  {{ postgres__create_schema(relation) }}
{% endmacro %}


{% macro splitgraph__drop_schema(relation) -%}
  {{ postgres__drop_schema(relation) }}
{% endmacro %}


{% macro splitgraph__get_columns_in_relation(relation) -%}
  {{ return(postgres__get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro splitgraph__list_relations_without_caching(schema_relation) %}
  {{ return(postgres__list_relations_without_caching(schema_relation)) }}
{% endmacro %}


{% macro splitgraph__information_schema_name(database) -%}
  {{ return(postgres__information_schema_name(database)) }}
{%- endmacro %}


{% macro splitgraph__list_schemas(database) -%}
  {{ return(postgres__list_schemas(database)) }}
{%- endmacro %}


{% macro splitgraph__check_schema_exists(information_schema, schema) -%}
  {{ return(postgres__check_schema_exists(information_schema, schema)) }}
{%- endmacro %}

{% macro splitgraph__current_timestamp() -%}
  {{ return(postgres__current_timestamp()) }}
{%- endmacro %}

{% macro splitgraph__snapshot_get_time() -%}
  {{ return(postgres__snapshot_get_time(timestamp)) }}
{%- endmacro %}


{% macro splitgraph__snapshot_string_as_time(timestamp) -%}
    {{ return(postgres__snapshot_string_as_time(timestamp)) }}
{%- endmacro %}

{% macro splitgraph__make_temp_relation(base_relation, suffix) %}
    {% do return(postgres__make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro splitgraph__alter_relation_comment(relation, comment) %}
  {% do return(postgres__alter_relation_comment(relation, comment)) %}
{% endmacro %}


{% macro splitgraph__alter_column_comment(relation, column_dict) %}
  {% do return(postgres__alter_column_comment(relation, column_dict)) %}
{% endmacro %}

