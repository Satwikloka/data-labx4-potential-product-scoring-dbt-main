{{ config(materialized='view') }}
SELECT DISTINCT
	pid.yearweek AS week_id,
	CAST (pid.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3,
	pid.is_eco,
	CASE 
		WHEN pid.product_impact <> 0 THEN 0
		ELSE 1
	END AS co2_eval,
	CASE 
		WHEN pid.product_impact <> 0 THEN pid.product_impact
		ELSE pid.avg_fam_impact
	END AS impact_co2
FROM {{source('cds_parquet', 'f_product_impact_detailed')}} pid
INNER JOIN 
(
SELECT 
	pid1.yearweek AS week_id,
	max(pid1.yearmonth) as d_day,
	CAST(pid1.mdl_num_model_r3 AS BIGINT) AS mdl_num_model_r3
FROM {{source('cds_parquet', 'f_product_impact_detailed')}} pid1
WHERE pid1.cnt_country_code_3a='FRA'
GROUP BY week_id, cnt_country_code_3a, mdl_num_model_r3
) t
	ON t.week_id = pid.yearweek AND t.mdl_num_model_r3 = pid.mdl_num_model_r3 AND t.d_day = pid.yearmonth
INNER JOIN {{source('cds_parquet', 'd_country')}} dc  
	ON pid.cnt_country_code_3a = dc.cnt_country_code_3a
WHERE 1=1
AND dc.cnt_country_code IN ('FR')

