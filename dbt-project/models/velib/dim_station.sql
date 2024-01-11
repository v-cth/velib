WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY station_code ORDER BY created_at DESC)
            AS rn
    FROM {{ ref("stg_bike_avaibility") }}
)

SELECT
    station_code,
    station_name,
    capacity,
    lon,
    lat,
    nom_arrondissement_communes AS city,
    is_renting,
    is_returning,
    COALESCE (sta.name IS NOT NULL, FALSE) AS station_plus
FROM ranked AS rkd
LEFT JOIN {{ ref('station_plus') }} AS sta ON rkd.station_name = sta.name
WHERE rkd.rn=1
ORDER BY station_code

