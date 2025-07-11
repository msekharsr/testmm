{% macro centralize_test_failures(results) %}
  {% if not results %}
    {{ log("No test results passed. Skipping centralize_test_failures.", info=True) }}
  {% endif %}
  {% for result in results if result.status == 'fail' %}
    {% set node = result.node %}
    {% set job_name = node.unique_id %}
    {% set model_name = node.name %}
    {% set error_type = node.test_metadata.name if node.test_metadata is defined else 'unknown_test' %}
    {% set data_source = node.relation_name if node.relation_name is defined else 'unknown_model' %}
    {% set error_detail = "Test `" ~ error_type ~ "` failed for model `" ~ model_name ~ "`" %}

    INSERT INTO {{ env_var('DBT_AUDIT_DB') }}.{{ env_var('DBT_AUDIT_SCHEMA') }}.ADT_FPA_ERR_DETAIL (
      audit_id, job_name, data_source, process_type, stage,
      severity, error_type, error_detail, insert_ts, load_id,
      extract_ts, integration_id
    ) VALUES (
      0, '{{ job_name }}', '{{ data_source | replace("'", "''") }}', 'Validation', 'SL',
      'High', '{{ error_type }}', '{{ error_detail | replace("'", "''") }}',
      CURRENT_TIMESTAMP, 1, CURRENT_TIMESTAMP, NULL
    );
  {% endfor %}
{% endmacro %}
