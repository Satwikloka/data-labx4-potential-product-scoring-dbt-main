{{
    config (
        materialized="incremental",
        incremental_strategy='append',
        file_format='delta',
        on_schema_change='sync_all_columns',
        post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY  mdl_num_model_r3;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
    )
}}

SELECT dsm_code, 
        CAST(ota.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
        brd_type_brand_libelle,
        brd_label_brand,
        mdl_label,
        ota.cnt_country_code,
        ota.day_week_start,
        ota.week_id,
        r3_unit_price AS euro_unit_price,
        ca_offline AS ca_offline_euro,
        margin_estimate_offline AS margin_offline_euro,
        qty_item_offline,
        nb_view,
        av_rate,
        count_stores,
        impact_co2,
        co2_eval,
        is_eco,
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
        current_timestamp() as _processing_time
FROM {{ref('f_offline_transactions')}} ota
LEFT JOIN {{ref('d_arbo_dmi')}} dmi 
    ON ota.mdl_num_model_r3 = dmi.mdl_num_model_r3
LEFT JOIN {{ref('d_r3_customers_review')}} rcr 
    ON ota.mdl_num_model_r3 = rcr.r3_code AND ota.cnt_country_code = rcr.country
LEFT JOIN {{ref('f_nb_stores_sold_r3')}} nssr 
    ON ota.mdl_num_model_r3 = nssr.mdl_num_model_r3  AND ota.cnt_country_code = nssr.cnt_country_code AND ota.week_id = nssr.week_id
LEFT JOIN {{ref('d_co2_impact')}} co2
    ON ota.mdl_num_model_r3  = co2.mdl_num_model_r3 AND ota.week_id = co2.week_id
WHERE ota.week_id < CONCAT(YEAR(DATE_SUB(current_date(), 31)), WEEKOFYEAR(DATE_SUB(current_date(), 31))) 
{% if is_incremental() %}
    AND ota.week_id > (select MAX(week_id) from {{ this }})
{% endif %} 
