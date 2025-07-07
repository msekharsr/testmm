{{ config(
    materialized='table',
    pre_hook=[
        "{{ insert_data_into_audit_table('INS', model.name, 'raw', none, 'SL_LOAD_START', 'sl', 'SAP', 'Y') }}"
    ],
    post_hook=[
        "{{ insert_data_into_audit_table('UPD', model.name, 'raw', 'SUCCESS', 'SL_LOAD_COMPLETE', 'sl', '', '') }}"
    ]
) }}

select
  policy_id,
  customer_id,
  policy_type,
  start_date,
  end_date,
  premium_amount
from {{ ref('raw_policies') }}
