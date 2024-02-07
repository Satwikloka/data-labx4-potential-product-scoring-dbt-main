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

SELECT
  dd.wee_id_week AS week_id,
  CAST(sku.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
  c.cnt_country_code,
  dcu.cur_code_currency,
  ce.exchange_rate AS exchange_rate,
  SUM(f_qty_item) AS qty_item_digital,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (SUM(tdc.f_margin_real) * ce.exchange_rate)
    ELSE SUM(tdc.f_margin_real) END AS margin_online_euro,
  CASE WHEN dcu.cur_code_currency <> 'EUR' THEN (SUM(tdc.f_to_tax_in) * ce.exchange_rate) 
    ELSE SUM(tdc.f_to_tax_in) END AS ca_digital
FROM
  {{ source('cds_parquet', 'f_delivery_detail') }} tdc
  LEFT JOIN {{ref('d_currency_exchange_euro')}} ce ON ce.id_currency = tdc.cur_idr_currency
  INNER JOIN {{ source('cds_parquet', 'd_sku') }} sku ON sku.sku_idr_sku = tdc.sku_idr_sku
  INNER JOIN {{ source('cds_parquet', 'd_business_unit') }} bu on bu.but_idr_business_unit = tdc.but_idr_business_unit_economical
  INNER JOIN {{ source('cds_parquet', 'd_day') }} dd ON CAST(tdc.the_date_transaction AS DATE) = dd.day_id_day
  INNER JOIN {{ source('cds_parquet', 'd_country') }} c ON tdc.cnt_idr_country = c.cnt_idr_country
  INNER JOIN {{source('cds_parquet', 'd_currency') }} dcu ON tdc.cur_idr_currency = dcu.cur_idr_currency
  INNER JOIN {{source('cds_parquet', 'd_reallocated_digital_type') }} drdt on drdt.rdt_idr_reallocated_digital_type = tdc.rdt_idr_reallocated_digital_type 
WHERE 1=1
  AND (CASE WHEN dcu.cur_code_currency <> 'EUR' THEN CURRENT_DATE() BETWEEN ce.start_date AND ce.end_date
    ELSE 1=1 END)
  AND drdt.rdt_reallocated_digital_type != 'DigitalInStore'
  AND c.cnt_country_code IN  {{var(target.name)['cz_europe_list']}} -- COUNTRY
  AND dd.wee_id_week < CONCAT(YEAR(DATE_SUB(current_date(), 31)), WEEKOFYEAR(DATE_SUB(current_date(), 31))) -- to avoid having the last month in the history
  {% if is_incremental() %}
  AND dd.wee_id_week > (select MAX(week_id) from {{ this }})  
  {% endif %}
  AND c.cnt_country_code IN {{ var(target.name)['cz_europe_list']}}
  AND year >= {{ var(target.name)['start_year']}}
GROUP BY
  1,
  2,
  3,
  4,
  5


