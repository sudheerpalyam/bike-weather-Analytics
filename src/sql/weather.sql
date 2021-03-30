create or replace database weather;

use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

create table json_weather_data (v variant);

create stage nyc_weather url = 's3://snowflake-workshop-lab/weather-nyc/weather_0';

list @nyc_weather;


copy into json_weather_data 
from @nyc_weather 
file_format = (type=json);

-- select * from json_weather_data limit 10;

create or replace view json_weather_data_view as
select
  v:time::timestamp as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;


-- select * from json_weather_data_view where date_trunc('month',observation_time) = '2018-01-01';

create or replace view weather_condition_trips as
select weather as conditions
    ,count(*) as num_trips
from citibike.public.trips 
left outer join json_weather_data_view
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;