
{{
    config (
        materialized="view",
    )
}}

SELECT * 
FROM {{ref('f_purchase_review_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_purchase_review_history')}}
