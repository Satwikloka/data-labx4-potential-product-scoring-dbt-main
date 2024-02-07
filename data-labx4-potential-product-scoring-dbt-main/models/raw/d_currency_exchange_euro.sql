{{ config(materialized='view') }}
SELECT
    cex.rate/cex.base AS exchange_rate,
    cex.from_currency AS from_currency,
    cex.start_date AS start_date,
    cex.end_date AS end_date,
    cur.cur_idr_currency AS id_currency
FROM {{source('cds_parquet', 'mtf_exchange_rate')}} AS cex
LEFT JOIN {{source('cds_parquet', 'd_currency')}} AS cur 
    ON cex.from_currency = cur.cur_code_currency
WHERE 1=1 
AND cex.rate_type = 'CLO'
AND cex.to_currency = 'EUR'

