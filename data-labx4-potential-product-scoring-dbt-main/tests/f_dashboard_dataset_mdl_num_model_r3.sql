
SELECT * FROM {{ref('f_dashboard_dataset')}}
WHERE REGEXP_LIKE(mdl_num_model_r3, '[^0-9]') OR length(mdl_num_model_r3) > 10