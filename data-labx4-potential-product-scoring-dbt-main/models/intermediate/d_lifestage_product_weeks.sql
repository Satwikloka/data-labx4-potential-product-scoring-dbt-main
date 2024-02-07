{{ config(materialized='view') }}

with lifestage_prodcut_tmp AS (
    SELECT 
        model_r3 as mdl_num_model_r3,
        min(day_id_day) as day_week_start,
        min(week_start) as week_start,
        max(week_end) as week_end
    FROM {{ref('d_lifestage_1')}} lf
    INNER JOIN {{ source('cds_parquet', 'd_day') }} dd 
    ON dd.wee_id_week = lf.week_start
    WHERE week_end >= 202100
    GROUP BY 1
)
SELECT 
    mdl_num_model_r3,
    week_start,
    week_end,
    day_week_start,
    min(day_id_day) as day_week_end
FROM lifestage_prodcut_tmp  lft
  INNER JOIN {{ source('cds_parquet', 'd_day') }} dd ON dd.wee_id_week = lft.week_end
GROUP BY 1, 2, 3, 4
