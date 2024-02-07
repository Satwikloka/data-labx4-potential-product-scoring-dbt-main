{{
    config (
        materialized="view",
    )
}}

SELECT *
FROM {{ref('f_dashboard_dataset_current')}} 
UNION ALL 
SELECT *
FROM
  {{ref('f_dashboard_dataset_history')}}