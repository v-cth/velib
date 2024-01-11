select 
cast(stationcode as integer) as station_code,
name as station_name,
case 
    when is_installed = 'OUI' then True 
    when is_installed = 'NON' then False
end as is_installed, 
capacity,
numdocksavailable,
mechanical,
ebike,
case 
    when is_renting='OUI' then True 
    when is_renting='NON' then False
end as is_renting,
case 
    when is_returning='OUI' then True 
    when is_returning='NON' then False
end as is_returning,
duedate, 
lon, 
lat, 
nom_arrondissement_communes,
created_at
from {{ source('velib', 'bike_avaibility') }} 
WHERE REGEXP_CONTAINS(stationcode, r'^\d{1,5}$') --stationcode contains between 1 and 5 digits
