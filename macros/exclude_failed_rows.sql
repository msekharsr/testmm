{% macro exclude_failed_rows(model) %}
    {% set pk_column = model.meta.get("pk") %}

    {% do log("Model: " ~ model.name ~ ", PK: " ~ pk_column, info=True) %}

    {% if pk_column is none %}
        {{ exceptions.raise_compiler_error("❌ No PK column found in model.meta for model: " ~ model.name) }}
    {% endif %}

    {% if not execute %}
        {{ return("SELECT * FROM " ~ ref(model.name)) }}
    {% endif %}

    {% set get_tables_query %}
        SELECT table_name
        FROM {{ target.database }}.information_schema.tables
        WHERE table_schema = '{{ target.schema | upper }}'
          AND table_name ILIKE 'dbt_test__%%'
    {% endset %}

    {% set table_result = run_query(get_tables_query) %}
    {% set test_tables = table_result.columns[0].values() if table_result else [] %}

    {% if test_tables | length == 0 %}
        {{ return("SELECT * FROM " ~ ref(model.name)) }}
    {% endif %}

    {% set union_queries = [] %}
    {% for table in test_tables %}
        {% set q = "SELECT DISTINCT " ~ pk_column ~ " AS pk FROM " ~ target.schema ~ "." ~ table %}
        {% do union_queries.append(q) %}
    {% endfor %}

    {% set failed_cte = "(" ~ union_queries | join(" UNION ") ~ ")" %}
    {% set final_query %}
        SELECT * FROM {{ ref(model.name) }} base
        WHERE {{ pk_column }} NOT IN (
            SELECT pk FROM {{ failed_cte }}
        )
    {% endset %}

    {{ return(final_query) }}
{% endmacro %}
