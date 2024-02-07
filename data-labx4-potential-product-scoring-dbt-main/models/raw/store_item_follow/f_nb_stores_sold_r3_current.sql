{{
    config (
        materialized="table",
        file_format='delta',
    )
}}

with store_item_follow_nested  as (
SELECT 
site_id, 
article_id, 
EXPLODE(availability_period) as availability_period
FROM 
{{ source('silver', 'store_item_follow') }}  

),
histo_tracked_article AS (
SELECT site_id, article_id,
CAST (availability_period.availability_starts as DATE) as availability_starts,
CAST (availability_period.availability_ends as DATE) as availability_ends
FROM store_item_follow_nested

)

SELECT
    week_id,
    CAST(mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
    cnt_country_code,
    COUNT(distinct but_num_business_unit) AS count_stores
FROM 
    (
        SELECT DISTINCT
            dd.wee_id_week AS week_id,
            CAST(ds.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
            dbu.cnt_country_code,
            dbu.but_num_business_unit
        FROM histo_tracked_article hta
            INNER JOIN {{ source('cds_parquet', 'd_day')}} dd
                ON  dd.day_id_day BETWEEN availability_starts AND availability_ends
            INNER JOIN {{ source('cds_parquet', 'd_business_unit')}} dbu
                ON dbu.but_num_business_unit = hta.site_id
            INNER JOIN {{ source('cds_parquet', 'd_sku')}} ds
                ON CAST(ds.sku_num_sku_r3 AS BIGINT) = CAST(LTRIM('0', hta.article_id) AS BIGINT)
        WHERE 1=1
        AND dbu.but_num_typ_but = 7
        AND dbu.but_closed = 0
        AND dbu.cnt_country_code IN {{var(target.name)['cz_europe_list']}}
        AND ((SELECT MAX(week_id) FROM {{ ref("f_nb_stores_sold_r3_history") }}) IS NULL OR dd.wee_id_week > (SELECT MAX(week_id) FROM {{ ref("f_nb_stores_sold_r3_history") }}) )
        AND dd.yea_id_year >= {{ var(target.name)['start_year']}}
        AND dd.day_id_day <= current_timestamp()
    )
GROUP BY week_id, mdl_num_model_r3, cnt_country_code
