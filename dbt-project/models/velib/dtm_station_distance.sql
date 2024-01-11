SELECT
    aa.station_code as origin_station_code,
    aa.station_name as origin_station_name,
    bb.station_code as dest_station_code,
    bb.station_name as dest_station_name,
  ST_DISTANCE(
    ST_GEOGPOINT(aa.lon, aa.lat),
    ST_GEOGPOINT(bb.lon, bb.lat)
  ) AS distance_meters
FROM
  {{ ref('dim_station') }} aa
  JOIN {{ ref('dim_station') }} bb on aa.station_code <> bb.station_code

