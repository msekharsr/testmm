{{ config(
    materialized='table',
    pre_hook=[
        "{{ insert_data_into_audit_table('INS', model.name, 'raw', none, 'SL_LOAD_START', 'sl', 'SAP', 'Y') }}"
    ],
    post_hook=[
        "{{ insert_data_into_audit_table('UPD', model.name, 'raw', 'SUCCESS', 'SL_LOAD_COMPLETE', 'sl', '', '') }}"
    ]
) }}

{% set pk_column = 'claim_id' %}  
{% set raw_table = ref('raw_claims') %}

with filtered as (
    {{ exclude_failed_rows(raw_table, pk_column) }}
)

select * from filtered
