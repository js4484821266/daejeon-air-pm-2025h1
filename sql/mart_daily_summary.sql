create table if not exists mart_daily_summary (
    day date not null,
    station text not null,
    pm10_avg real,
    pm25_avg real,
    row_count integer not null,
    primary key (day, station)
);
-- load from raw_pm by day & by station
insert into mart_daily_summary (day, station, pm10_avg, pm25_avg, row_count)
select
measured_at::date as day, station, avg(pm10) as pm10_avg, avg(pm25) as pm25_avg, count(*) as row_count
from raw_pm group by measured_at::date, station
on conflict (day, station) do update set
pm10_avg=excluded.pm10_avg,
pm25_avg=excluded.pm25_avg,
row_count=excluded.row_count;