select temp
from {{ref('stg_weather')}}
where temp > 60