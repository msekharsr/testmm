{{ config(
    materialized = 'view',
    pre_hook = [
      "{{ insert_data_into_audit_table('INS', model.name, 'sl', none, 'GL_LOAD_START', 'gl', 'SAP', 'Y') }}"
    ],
    post_hook = [
      "{{ insert_data_into_audit_table('UPD', model.name, 'sl', 'SUCCESS', 'GL_LOAD_COMPLETE', 'gl', '', '') }}"
    ]
) }}

SELECT
  p.policy_id,
  p.customer_id,
  p.policy_type,
  p.start_date,
  p.end_date,
  p.premium_amount,
  c.claim_id,
  c.claim_date,
  c.claim_amount,
  c.claim_status
FROM {{ ref('sl_policies') }} p
LEFT JOIN {{ ref('sl_claims') }} c
  ON p.policy_id = c.policy_id
