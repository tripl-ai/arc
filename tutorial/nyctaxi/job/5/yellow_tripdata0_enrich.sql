SELECT 
  yellow_tripdata0.vendor_name
  ,vendor_id.vendor_id
  ,trip_pickup_datetime AS pickup_datetime
  ,trip_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,start_lon AS pickup_longitude
  ,start_lat AS pickup_latitude
  ,end_lon AS dropoff_longitude
  ,end_lat AS dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amt AS fare_amount
  ,surcharge AS extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,NULL AS ehail_fee
  ,NULL AS improvement_surcharge
  ,total_amount
  ,LOWER(yellow_tripdata0.payment_type) AS payment_type
  ,payment_type_id.payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_nyct2010_gid
  ,NULL AS dropoff_nyct2010_gid
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM yellow_tripdata0
LEFT JOIN vendor_id ON yellow_tripdata0.vendor_name = vendor_id.vendor
LEFT JOIN payment_type_id ON LOWER(yellow_tripdata0.payment_type) = payment_type_id.payment_type