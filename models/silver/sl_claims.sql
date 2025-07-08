-- depends_on: {{ ref('sl_claims') }}
{{ config(
    materialized='table',
    pre_hook=[
      "{{ insert_data_into_audit_table('INS', model.name, 'raw', none, 'SL_LOAD_START', 'sl', 'SAP', 'Y') }}"
    ],
    post_hook=[
      "{{ insert_data_into_audit_table('UPD', model.name, 'raw', 'SUCCESS', 'SL_LOAD_COMPLETE', 'sl', '', '') }}"
    ]
) }}

with filtered as (
    {{ exclude_failed_rows(model) }}
)

select * from filtered
