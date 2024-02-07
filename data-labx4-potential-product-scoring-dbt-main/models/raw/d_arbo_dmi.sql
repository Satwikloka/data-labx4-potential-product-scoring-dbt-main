{{
    config (
        materialized="view"
        
    )
}}
SELECT DISTINCT
  ds.dsm_code AS super_model,
  CAST(dh.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
  CASE WHEN ds.brd_type_brand_libelle is NULL THEN dsh.brd_type_brand_libelle 
    ELSE ds.brd_type_brand_libelle END AS brd_type_brand_libelle,
  CASE WHEN ds.brd_label_brand is NULL THEN dsh.brd_label_brand 
    ELSE ds.brd_label_brand END AS brd_label_brand,
  CASE WHEN ds.mdl_label is NULL THEN dsh.mdl_label 
    ELSE ds.mdl_label END AS mdl_label,
  CAST(dh.niv_unv as VARCHAR(10)) as sector_purch_id,
  dh.unv_label AS sector_purch_label,
  CAST(dh.niv_ray as VARCHAR(10)) as departement_purch_id,
  dh.ray_label AS departement_purch_label,
  CAST(dh.niv_sr as VARCHAR(10)) as subdepart_purch_id,
  dh.sr_label AS subdepart_purch_label,
  CAST(dh.niv_fa as VARCHAR(10)) as merchandise_category_id,
  dh.fa_label AS merchandise_category_label,
  CAST(ds.pnt_num_product_nature as VARCHAR(10)) as merch_cat_prod2_id,
  CASE WHEN ds.product_nature_label is NULL OR ds.product_nature_label = "UNDEFINED" THEN dsh.product_nature_label 
    ELSE ds.product_nature_label END AS merch_cat_prod2_label
FROM {{source("cds_parquet", "d_sku")}} ds
  INNER JOIN {{source("cds_parquet", "d_hierarchy_supply")}} dh
    ON CAST(dh.mdl_num_model_r3 AS BIGINT) = ds.mdl_num_model_r3 
  INNER JOIN (
    SELECT 
    CAST(mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3, 
    MIN(brd_type_brand_libelle) AS brd_type_brand_libelle,
    MIN(brd_label_brand) AS brd_label_brand, 
    MIN(mdl_label) as mdl_label, 
    MIN(product_nature_label) as product_nature_label
from {{source("cds_parquet", "d_sku_h")}}
where 1=1
AND mdl_label is not null 
AND product_nature_label is not null 
AND product_nature_label is not null 
AND product_nature_label <> 'UNDEFINED'
GROUP BY mdl_num_model_r3
  ) dsh ON ds.mdl_num_model_r3 = dsh.mdl_num_model_r3
WHERE 1=1
AND dh.mdl_num_model_r3 IS NOT NULL
AND dsh.mdl_label <> ""
AND dh.unv_label <> ""
AND dh.ray_label <> ""
AND dh.sr_label <> ""
AND dh.fa_label <> ""
AND ds.product_nature_label <> ""
AND dh.org_fa=1