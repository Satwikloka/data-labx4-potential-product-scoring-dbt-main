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
WITH ga_filtered AS (
    SELECT  visitId, 
            TO_DATE(DATE, 'yyyyMMdd') AS ddate, 
            fullVisitorId, 
            EXPLODE(hits) AS hits, 
            partition_0 as country
    FROM  {{ source('analytics', 'ga_ecommerce') }} AS g 
    WHERE 
    partition_0 IN {{ var(target.name)['cz_europe_list']}}
    {% if is_incremental() %}
        AND TO_DATE(CONCAT_WS("-", year, LPAD(month, 2, 0), LPAD(day, 2, 0)), 'yyyy-MM-dd') > (select MAX(ddate) from {{ this }}) 
    {% endif %}
    AND year >= {{ var(target.name)['start_year']}}
    ), 
ga_exploded AS (
    SELECT 
        visitId, 
        ddate, 
        fullVisitorId, 
        country, 
        hits.transaction.transactionId AS transactionID,
        CAST (hits.eCommerceAction.action_type AS INT) AS action_type,
        EXPLODE(hits.product) AS product
    FROM ga_filtered
    ), 
ga_enriched AS (
    SELECT 
        ddate, 
        transactionID,
        country,
        action_type, 
        product.productSKU AS productSKU,
        CONCAT(visitId, '-', FullVisitorId ) AS visitUniqueId
    FROM ga_exploded
    ),
ga_tmp AS (
    SELECT  
        ddate, 
        transactionID,
        country,
        action_type,
        productSKU,
        visitUniqueId,
            CASE
                    WHEN action_type=2 THEN 'product_view' -- product page viewed
                    WHEN action_type=6 AND transactionID IS NOT NULL THEN 'product_purchase' -- product purchased
                    ELSE "uncategorized" 
            END AS action_label
    FROM ga_enriched 

)

SELECT 
productSKU,
visitUniqueId,
ddate, 
country,
transactionID,
action_label, 
current_timestamp() as _processing_time
FROM ga_tmp
WHERE  action_label != 'uncategorized'



