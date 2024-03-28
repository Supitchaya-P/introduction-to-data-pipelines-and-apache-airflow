select avg(temp) as avg_temp
from {{ref('stg_weathers')}}
--select 31.39 avg_tmp
