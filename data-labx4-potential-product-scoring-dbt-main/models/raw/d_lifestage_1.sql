{{ config(materialized='view') }}
SELECT 
    sales_org_text,
    date_begin,
    date_end,
	start_day.wee_id_week as week_start,
	end_day.wee_id_week as week_end,
	CAST(LTRIM('0', material_id) AS BIGINT) as model_r3,
    lifestage,
	current_timestamp() as _processing_time
FROM
	(
	SELECT
		material_id,
		sales_org,
		distrib_channel,
		date_begin,
		CASE
			WHEN date_end = '2999-12-31' THEN '2030-12-31'
			ELSE date_end
		END AS date_end,
		lifestage
	FROM
		{{source('cds_parquet', 'd_sales_data_material_h')}}
) dsdmh
INNER JOIN {{source('cds_parquet', 'sales_organizations_texts')}} sot on
	sot.sales_org = dsdmh.sales_org
INNER JOIN 
(
	SELECT
		day_id_day,
		wee_id_week
	FROM
		{{source('cds_parquet', 'd_day')}}  dd
) start_day on
	start_day.day_id_day = dsdmh.date_begin
INNER JOIN
(
	SELECT
		day_id_day,
		wee_id_week
	FROM
		{{source('cds_parquet', 'd_day')}} dd
) end_day on
	end_day.day_id_day = dsdmh.date_end
INNER JOIN {{source('cds_parquet', 'd_label_translation')}} lt on
    upper(lt.lat_long_label) = SPLIT(sot.sales_org_text,'SO ')[1]
WHERE
	lifestage = 1
	AND distrib_channel = '02'
  AND  sap_source = 'PRT'
  AND material_id is not null
	AND  sales_org_text IN {{var(target.name)['so_cz_europe_list']}}
GROUP BY
	1,
	2,
	3,
	4,
	5,
	6,
	7




  -- when joining with other table be sure the date is [date_begin, date_end]