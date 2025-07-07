{% macro insert_failed_tests(results) %}
  {% set failures = results | selectattr('status', 'equalto', 'fail') | list %}

  {% if failures | length > 0 %}
    {% do log("Starting failure insert macro...", info=True) %}

    {% for result in failures %}
      {% set node = result.node %}
      {% set test_name = node.name %}
      {% set error_type = node.test_metadata.name if node.test_metadata is defined else 'unknown' %}
      {% set relation = node.relation_name %}

      {% if relation is not none %}
        {% do log("Inserting from test table: " ~ relation, info=True) %}

        INSERT INTO {{ env_var('DBT_AUDIT_DB') }}.{{ env_var('DBT_AUDIT_SCHEMA') }}.FAILED_TEST_RECORDS (
          job_name,
          error_type,
          failure_data,
          inserted_at
        )
        select
          '{{ test_name }}',
          '{{ error_type }}',
          to_variant(object_construct(*)),
          current_timestamp
        from {{ relation }};

      {% else %}
        {% do log("Skipping test: " ~ test_name ~ " — no failure table found.", info=True) %}
      {% endif %}
    {% endfor %}

    {% do log("Failure insert macro completed.", info=True) %}
  {% else %}
    {% do log("No failed tests to insert.", info=True) %}
  {% endif %}
{% endmacro %}
