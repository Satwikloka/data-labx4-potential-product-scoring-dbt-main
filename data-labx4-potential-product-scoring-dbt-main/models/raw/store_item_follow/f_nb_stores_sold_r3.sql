
{{
    config (
        materialized="view",
    )
}}

SELECT * 
FROM {{ref('f_nb_stores_sold_r3_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_nb_stores_sold_r3_history')}}
