{{ config(
    materialized='table',
    pre_hook=[
      "{{ insert_data_into_audit_table('INS', model.name, 'raw', none, 'SL_LOAD_START', 'sl', 'SAP', 'Y') }}"
    ],
    post_hook=[
      "{{ insert_data_into_audit_table('UPD', model.name, 'raw', 'SUCCESS', 'SL_LOAD_COMPLETE', 'sl', '', '') }}",
    ]
) }}


SELECT
    claim_id,
    policy_id,
    claim_date,
    claim_amount,
    claim_status
FROM {{ ref('raw_claims') }}
