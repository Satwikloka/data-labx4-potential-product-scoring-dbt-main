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

SELECT 
    product_id AS productSKU,
    CONCAT(visit_id, '-', visitor_id) AS visitUniqueId, 
    TO_DATE(CONCAT_WS("-", year, LPAD(month, 2, 0), LPAD(day, 2, 0)), 'yyyy-MM-dd') AS ddate,
    transaction_ID AS transactionID,
    CASE 
        WHEN event_name = 'product.page_display' THEN 'product_view'
        WHEN event_name = 'product.purchased' THEN 'product_purchase'
        ELSE 'uncategorized'
    END AS action_label,
    partition_0 as country, 
    current_timestamp() as _processing_time
FROM {{ source('analytics', 'piano_analytics_ecommerce') }} pan 
WHERE 
    NOT event_name NOT IN ('product.page_display', 'product.purchased')
    {% if is_incremental() %}
        AND TO_DATE(CONCAT_WS("-", year, LPAD(month, 2, 0), LPAD(day, 2, 0)), 'yyyy-MM-dd') > (select MAX(ddate) from {{ this }}) 
    {% endif %}
    AND year >= {{ var(target.name)['start_year']}}

