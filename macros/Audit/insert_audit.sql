{% macro insert_data_into_audit_table(load_type=None, job_name=None, source_name=None,
                             status=None, message=None, stage=None,
                             integration_id=None, increment_flag=None) %}
  {% set audit_table = (env_var('DBT_AUDIT_DB') ~ '.' ~ env_var('DBT_AUDIT_SCHEMA') ~ '.adt_fpa_audit_' ~ source_name) %}

  {% set lt = load_type or 'INS' %}
  {% set jn = job_name or 'NA' %}
  {% set sn = source_name %}
  {% set stg = stage or 'NA' %}
  {% set inc = increment_flag or 'Y' %}

  {% if execute %}
    {% if lt | upper == 'INS' %}
      {% set pt = message %}  {# For INS, message is actually process_type #}
      {%- call statement('get_audit_id', fetch_result=True) -%}
        select coalesce(max(id), 0) + 1 from {{ audit_table }}
      {%- endcall -%}
      {% set aid = load_result('get_audit_id')['data'][0][0] %}

      {%- call statement('get_load_id', fetch_result=True) -%}
        select coalesce(max(case when '{{inc}}' = 'N' then load_id else load_id + 1 end), 0)
        from {{ audit_table }}
        where data_source = '{{ sn }}' and lower(status) = 'success'
      {%- endcall -%}
      {% set lid = load_result('get_load_id')['data'][0][0] %}

      {% set sql %}
      insert into {{ audit_table }} (
        id, job_name, data_source, process_type, stage,
        start_ts, load_id, insert_ts, integration_id
      ) values (
        {{aid}}, '{{jn}}', '{{sn}}', '{{pt}}', '{{stg}}',
        current_timestamp, {{lid}}, current_timestamp,
        {% if integration_id %}'{{integration_id}}'{% else %}null{% endif %}
      );
      {% endset %}
      {% do run_query(sql) %}

    {% elif lt | upper == 'UPD' %}
      {% set st = status %}
      {% set msg = message %}
      {% set sql %}
      update {{ audit_table }}
      set status = '{{st}}', message = '{{msg}}', end_ts = current_timestamp
      where job_name = '{{jn}}'
        and stage = '{{stg}}'
        and id = (
          select max(id)
          from {{ audit_table }}
          where data_source = '{{sn}}'
        );
      {% endset %}
      {% do run_query(sql) %}

    {% else %}
      {{ exceptions.raise_compiler_error('Invalid load_type: ' ~ lt) }}
    {% endif %}
  {% endif %}
{% endmacro %}
