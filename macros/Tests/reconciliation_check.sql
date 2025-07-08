-- macros/reconciliation_check.sql

{% test reconciliation_check(model, source_model, join_keys, threshold_percent=0) %}
  {% set model_name = model.name %}
  {% set source_model_name = source_model %}
  {% set pk_list = join_keys | join(', ') %}

  with 
  src as (
    select * from {{ source_model_name }}
  ),
  tgt as (
    select * from {{ model }}
  ),
  stats as (
    select
      (select count(*) from src) as src_count,
      (select count(*) from tgt) as tgt_count,
      (select count(distinct {{ pk_list }}) from src) as src_distinct,
      (select count(distinct {{ pk_list }}) from tgt) as tgt_distinct
  )

  select *
  from stats
  where
    case
      when src_count = 0 then tgt_count != 0
      else
        abs(src_count - tgt_count) * 100.0 / src_count > {{ threshold_percent }}
        or abs(src_distinct - tgt_distinct) * 100.0 / src_distinct > {{ threshold_percent }}
    end

{% endtest %}
