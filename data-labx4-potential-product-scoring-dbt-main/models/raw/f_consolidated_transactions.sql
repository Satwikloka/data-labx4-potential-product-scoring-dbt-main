{{
    config (
        materialized="incremental",
        incremental_strategy='append',
        file_format='delta',
        on_schema_change='sync_all_columns',
        post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY productSKU;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
    )
}}

{%set action_labels = [ 'product_view', 'product_purchase'] %}

WITH ga_piano_tmp AS (
    SELECT 
        productSKU,
        visitUniqueId,
        ddate AS day_id_day, 
        country,
        transactionID,
        action_label
    FROM {{ref('f_google_analytics')}} as ga
    {% if is_incremental() %}
        WHERE ga._processing_time > (select MAX(_processing_time) from {{ this }})
    {% endif %} 
    UNION
    SELECT
        productSKU,
        visitUniqueId,
        ddate AS day_id_day,
        country,
        transactionID,
        action_label
    FROM {{ref('f_piano_analytics')}} as pia 
    {% if is_incremental() %}
        WHERE pia._processing_time > (select MAX(_processing_time) from {{ this }})
    {% endif %} 
    ),
tmp AS (
    SELECT 
        productSKU,
        visitUniqueID,
        CAST(day_id_day AS DATE) AS day_id_day,
        country,
        transactionID,
        action_label
    FROM ga_piano_tmp
)
SELECT 
    CAST(tmp.productSKU AS BIGINT) AS productSKU,
    visitUniqueId,
    transactionID,
    country,
    dd.day_id_day,
    wee_id_week AS week_id,
    {% for label in action_labels %}
    MAX(CASE WHEN action_label='{{label}}' THEN 1 ELSE 0 END) as has_{{label}}
    {% if not loop.last %},{% endif %}
    {% endfor %},
    current_timestamp() AS _processing_time
FROM tmp
LEFT JOIN {{source("cds_parquet", "d_day")}} AS dd 
    ON tmp.day_id_day == dd.day_id_day
GROUP BY 1,2,3,4,5,6
  