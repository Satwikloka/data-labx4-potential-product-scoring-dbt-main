{{ config(materialized='table',
 file_format='delta',)
 }}

SELECT CAST(ors.mdl_num_model_r3 AS VARCHAR(20)),
        dsm_code,
        brd_type_brand_libelle,
        brd_label_brand,
        mdl_label,
        ors.day_week_start,
        ors.week_id,
        ors.cnt_country_code,
        euro_unit_price,
        ca_offline_euro,
        margin_offline_euro,
        qty_item_offline,
        pvd.product_qty_digital,
        pvd.product_margin_digital,
        pvd.product_revenue_digital,
        nb_view,
        av_rate,
        count_stores,
        impact_co2,
        co2_eval,
        is_eco,
        pvd.has_product_view,
        pvd.has_product_purchase,
        pvd.unique_product_visits_trans,
        sector_purch_id,
        sector_purch_label,
        departement_purch_id,
        departement_purch_label,
        subdepart_purch_id,
        subdepart_purch_label,
        merchandise_category_id,
        merchandise_category_label,
        merch_cat_prod2_id,
        merch_cat_prod2_label, 
        CASE
            WHEN ors.week_id >= week_start AND ors.week_id <= week_end THEN 1
            ELSE 0
        END AS flag_lifestage1,
        current_timestamp() as _processing_time
FROM {{ref("f_offline_review_nb_stores_co2")}} ors
INNER JOIN {{ref("d_lifestage_product_weeks")}} lfw
    ON lfw.mdl_num_model_r3 == ors.mdl_num_model_r3  
LEFT JOIN {{ref('f_products_visit_dmi_agg')}} pvd 
    ON ors.mdl_num_model_r3 == pvd.productSKU AND ors.cnt_country_code == pvd.country AND ors.week_id == pvd.week_id
WHERE ors.week_id > (SELECT MAX(week_id) FROM {{ ref("f_dashboard_dataset_history") }})