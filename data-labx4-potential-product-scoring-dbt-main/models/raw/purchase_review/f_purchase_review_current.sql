{{
    config (
        materialized="table",
        file_format='delta',
    )
}}

WITH products_visits AS (
SELECT 
   CAST(ct.productSKU AS BIGINT) AS productSKU,
    ct.visitUniqueID,
    ct.week_id,
    ct.country,
    ct.has_product_purchase,
    MAX(CASE WHEN ct.has_product_purchase=1 OR ct.has_product_view=1 THEN 1 ELSE 0 END) AS has_product_view
FROM {{ref('f_consolidated_transactions')}} ct
WHERE 1=1
AND ((SELECT MAX(week_id) FROM {{ ref("f_purchase_review_history") }}) IS NULL OR ct.week_id > (SELECT MAX(week_id) FROM {{ ref("f_purchase_review_history") }}) )
GROUP BY ct.productSKU, ct.visitUniqueID, ct.week_id, ct.has_product_purchase, ct.has_product_view, ct.country
)

SELECT 
        productSKU,
        week_id,
        country,
        SUM(has_product_view) AS has_product_view,
        SUM(has_product_purchase) AS has_product_purchase,
        SUM(has_product_view * has_product_purchase) as unique_product_visits_trans 
FROM products_visits
WHERE 1=1
AND week_id IS NOT NULL 
GROUP BY productSKU, week_id, country