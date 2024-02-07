
{{
    config (
        materialized="view",
    )
}}

SELECT * 
FROM {{ref('f_offline_transactions_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_offline_transactions_history')}}
