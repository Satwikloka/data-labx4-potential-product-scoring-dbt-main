{{ config(materialized='view') }}
SELECT 
  CAST(ocp.code AS BIGINT) as r3_code,
  country_reference AS country,
  COUNT(orr.id) AS nb_view,
  ROUND(AVG(CAST (note as FLOAT)), 3) av_rate
FROM 
  {{ source('ods_parquet', 'opv_review__review') }}  orr
  INNER JOIN  {{ source('ods_parquet', 'opv_catalog__product') }}  ocp
  ON orr.product_reference_id = ocp.id
WHERE country_reference IN {{var(target.name)['cz_europe_list']}}
  AND product_reference_id IS NOT NULL  
  AND published_at IS  NOT NULL 
  AND unpublished_at IS NULL
  AND published_at >= DATEADD(year,-1,current_date)
GROUP BY 
  ocp.code,
  country_reference


