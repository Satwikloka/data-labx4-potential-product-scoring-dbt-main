SELECT * FROM {{ref('f_dashboard_dataset')}}
WHERE length(cnt_country_code) <>2 