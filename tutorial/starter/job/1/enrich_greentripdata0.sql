SELECT 
    *
    ,HOUR(lpep_pickup_datetime) AS pickup_hour
    ,DAYOFWEEK(lpep_pickup_datetime) AS pickup_dayofweek
    ,UNIX_TIMESTAMP(lpep_dropoff_datetime) - UNIX_TIMESTAMP(lpep_pickup_datetime) AS duration
    ,CASE
        WHEN 
            (pickup_latitude < 40.651381 
            AND pickup_latitude > 40.640668
            AND pickup_longitude < -73.776283
            AND pickup_longitude > -73.794694)
            OR
            (dropoff_latitude < 40.651381 
            AND dropoff_latitude > 40.640668
            AND dropoff_longitude < -73.776283
            AND dropoff_longitude > -73.794694)           
        THEN 1 
        ELSE 0
    END AS jfk
FROM green_tripdata0
WHERE trip_distance > 0
AND pickup_longitude IS NOT NULL
AND pickup_latitude IS NOT NULL
AND dropoff_longitude IS NOT NULL
AND dropoff_latitude IS NOT NULL