{{
    config (
        materialized="view",
    )
}}

SELECT * 
FROM {{ref('f_products_visit_dmi_agg_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_products_visit_dmi_agg_history')}}