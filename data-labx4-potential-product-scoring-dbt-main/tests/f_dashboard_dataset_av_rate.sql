SELECT * FROM {{ref('f_dashboard_dataset')}}
WHERE av_rate < 0 OR av_rate > 5 
