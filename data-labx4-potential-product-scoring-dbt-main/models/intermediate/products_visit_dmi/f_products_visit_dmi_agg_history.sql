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
    fot.mdl_num_model_r3 AS productSKU,
    fot.week_id,
    fot.cnt_country_code AS country,
    fot.qty_item_digital AS product_qty_digital,
    fot.margin_online_euro AS product_margin_digital,
    fot.ca_digital AS product_revenue_digital,
    SUM(has_product_view) AS has_product_view,
    SUM(has_product_purchase) AS has_product_purchase,
    SUM(has_product_view * has_product_purchase) AS unique_product_visits_trans 
FROM {{ref('f_online_transactions')}} fot
LEFT JOIN {{ref('f_purchase_review')}} fpr
    ON fot.mdl_num_model_r3 = fpr.productSKU
    AND fot.week_id = fpr.week_id
    AND fot.cnt_country_code = fpr.country
WHERE fot.week_id IS NOT NULL 
AND fot.week_id < CONCAT(YEAR(DATE_SUB(current_date(), 31)), WEEKOFYEAR(DATE_SUB(current_date(), 31)))
{% if is_incremental() %}
    AND fot.week_id > (select MAX(week_id) from {{ this }})
{% endif %} 
GROUP BY 1,2,3,4,5,6
