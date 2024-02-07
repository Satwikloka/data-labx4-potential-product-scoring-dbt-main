{{
    config (
        materialized="view",
    )
}}

SELECT *
FROM {{ref('f_offline_review_nb_stores_co2_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_offline_review_nb_stores_co2_history')}}