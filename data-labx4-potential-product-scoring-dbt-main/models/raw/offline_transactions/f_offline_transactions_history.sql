
{{
    config (
        materialized="incremental",
        incremental_strategy='append',
        file_format='delta',
        on_schema_change='sync_all_columns',
        post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY week_id, cnt_country_code;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
    )
}}

with offline_transactions_tmp AS (
SELECT 
  dd.wee_id_week AS week_id,
  dc.cnt_country_code,
  ftd.cur_idr_currency,
  dcu.cur_code_currency,
  sku.dsm_code,
  CAST(sku.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
  ce.exchange_rate AS exchange_rate,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (ROUND(AVG(f_pri_regular_sales_unit), 2) * ce.exchange_rate)
    ELSE ROUND(AVG(f_pri_regular_sales_unit), 2) END AS r3_unit_price,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (ROUND(SUM(f_to_tax_in), 2) * ce.exchange_rate) 
    ELSE ROUND(SUM(f_to_tax_in), 2) END AS ca_offline,
  SUM(f_qty_item) as qty_item_offline,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (ROUND(SUM(f_margin_estimate), 2) * ce.exchange_rate)
    ELSE ROUND(SUM(f_margin_estimate), 2) END AS margin_estimate_offline
FROM {{ source('cds_parquet', 'f_transaction_detail') }} ftd
INNER JOIN {{ source('cds_parquet', 'd_country') }}  dc ON dc.cnt_idr_country = ftd.cnt_idr_country
INNER JOIN {{ source('cds_parquet', 'd_sku') }}  sku ON sku.sku_idr_sku = ftd.sku_idr_sku
INNER JOIN {{ source('cds_parquet', 'd_day') }}  dd ON CAST (ftd.tdt_date_to_ordered AS DATE) = dd.day_id_day
INNER JOIN {{source('cds_parquet', 'd_currency') }} dcu ON ftd.cur_idr_currency = dcu.cur_idr_currency
LEFT JOIN {{ref('d_currency_exchange_euro')}} ce ON ce.id_currency = ftd.cur_idr_currency
WHERE 1 = 1
  AND (CASE WHEN dcu.cur_code_currency <> 'EUR' THEN CURRENT_DATE() BETWEEN ce.start_date AND ce.end_date
    ELSE 1=1 END)
AND dc.cnt_country_code IN {{var(target.name)['cz_europe_list']}}
AND the_to_type = 'offline'
AND dd.wee_id_week < CONCAT(YEAR(DATE_SUB(current_date(), 31)), WEEKOFYEAR(DATE_SUB(current_date(), 31))) -- to avoid having the last month in the history
{% if is_incremental() %}
AND dd.wee_id_week > (select MAX(week_id) from {{ this }})  
{% endif %}
AND year >= {{ var(target.name)['start_year']}}
GROUP BY 1,2,3,4,5,6,7
UNION ALL
SELECT
  dd.wee_id_week AS week_id,
  dc.cnt_country_code,
  tdc.cur_idr_currency,
  dcu.cur_code_currency,
  sku.dsm_code,
CAST(sku.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3 ,
  ce.exchange_rate AS exchange_rate,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (ROUND(AVG(f_tdt_pri_regular_sales_unit), 2) * ce.exchange_rate)
    ELSE ROUND(AVG(f_tdt_pri_regular_sales_unit), 2) END AS r3_unit_price,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (SUM(tdc.f_to_tax_in) * ce.exchange_rate) 
    ELSE SUM(tdc.f_to_tax_in) END AS ca_digital,
  SUM(tdc.f_qty_item) AS qty_item_digital,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (SUM(tdc.f_margin_real) * ce.exchange_rate)
    ELSE SUM(tdc.f_margin_real) END AS margin_online_euro
  FROM {{ source('cds_parquet', 'f_delivery_detail') }} tdc
  LEFT JOIN {{ref('d_currency_exchange_euro')}} ce ON ce.id_currency = tdc.cur_idr_currency
  INNER JOIN {{ source('cds_parquet', 'd_sku') }} sku ON sku.sku_idr_sku = tdc.sku_idr_sku
  INNER JOIN {{ source('cds_parquet', 'd_business_unit') }} bu on bu.but_idr_business_unit = tdc.but_idr_business_unit_economical
  INNER JOIN {{ source('cds_parquet', 'd_day') }} dd ON CAST(tdc.tdt_date_to_ordered AS DATE) = dd.day_id_day
  INNER JOIN {{ source('cds_parquet', 'd_country') }} dc ON tdc.cnt_idr_country = dc.cnt_idr_country
  INNER JOIN {{source('cds_parquet', 'd_currency') }} dcu ON tdc.cur_idr_currency = dcu.cur_idr_currency
  INNER JOIN {{source('cds_parquet', 'd_reallocated_digital_type') }} drdt on drdt.rdt_idr_reallocated_digital_type = tdc.rdt_idr_reallocated_digital_type 
WHERE 1=1
AND (CASE WHEN dcu.cur_code_currency <> 'EUR' THEN CURRENT_DATE() BETWEEN ce.start_date AND ce.end_date
    ELSE 1=1 END)
AND drdt.rdt_reallocated_digital_type = 'DigitalInStore'
AND dc.cnt_country_code IN {{var(target.name)['cz_europe_list']}}
AND dd.wee_id_week < CONCAT(YEAR(DATE_SUB(current_date(), 31)), WEEKOFYEAR(DATE_SUB(current_date(), 31))) -- to avoid having the last month in the history
{% if is_incremental() %}
AND dd.wee_id_week > (select MAX(week_id) from {{ this }})  
{% endif %}
AND year >= {{ var(target.name)['start_year']}}
GROUP BY 1,2,3,4,5,6,7
)

 -- CLEAN OFF LINE TRANSACTIONAL DATA

SELECT 
  otp.week_id,
  md.day_week_start,
  cnt_country_code,
  cur_idr_currency,
  cur_code_currency,
  dsm_code,
  mdl_num_model_r3,
  AVG(r3_unit_price) AS r3_unit_price,
  SUM(ca_offline) AS ca_offline,
  SUM(qty_item_offline) AS qty_item_offline,
  SUM(margin_estimate_offline) AS margin_estimate_offline,
  AVG(exchange_rate) AS exchange_rate
FROM offline_transactions_tmp otp
INNER JOIN (
  SELECT 
    wee_id_week AS week_id,
    min(day_id_day) AS day_week_start
  FROM {{ source('cds_parquet', 'd_day') }}
  GROUP BY wee_id_week
  ) md ON md.week_id = otp.week_id
GROUP BY 1,2,3,4,5,6,7
ORDER BY week_id,cnt_country_code

