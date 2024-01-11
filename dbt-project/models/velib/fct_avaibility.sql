select
    station_code,
    station_name,
    capacity,
    numdocksavailable as docks_available,
    mechanical,
    ebike,
    duedate,
    mechanical + ebike as bikes_available,
    SAFE_DIVIDE(mechanical + ebike, capacity) as rate_bike_available,
    --need the complete list of station plus to exclude them, have negative #
    SAFE_DIVIDE(numdocksavailable, capacity) as rate_dock_available,
    capacity - numdocksavailable - ebike - mechanical as bikes_out_of_order,
    DATETIME(created_at, 'Europe/Paris') as collected_at, --paris time
    DATE_TRUNC(DATETIME(created_at, 'Europe/Paris'), hour)
        as rounded_collected_at
from {{ ref("stg_bike_avaibility") }}
where is_installed and is_renting and is_returning
