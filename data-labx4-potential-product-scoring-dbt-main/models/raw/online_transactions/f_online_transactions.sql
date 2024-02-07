
{{
    config (
        materialized="view",
    )
}}

SELECT * 
FROM {{ref('f_online_transactions_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_online_transactions_history')}}
