SELECT 
  vendor_id
  ,lpep_pickup_datetime AS pickup_datetime
  ,lpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,pickup_longitude
  ,pickup_latitude
  ,dropoff_longitude
  ,dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,ehail_fee
  ,NULL AS improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM green_tripdata0

UNION ALL

SELECT 
  vendor_id
  ,lpep_pickup_datetime AS pickup_datetime
  ,lpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,pickup_longitude
  ,pickup_latitude
  ,dropoff_longitude
  ,dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,ehail_fee
  ,improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM green_tripdata1

UNION ALL

SELECT 
  vendor_id
  ,lpep_pickup_datetime AS pickup_datetime
  ,lpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,NULL AS pickup_longitude
  ,NULL AS pickup_latitude
  ,NULL AS dropoff_longitude
  ,NULL AS dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,ehail_fee
  ,improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,pickup_location_id
  ,dropoff_location_id
FROM green_tripdata2

UNION ALL

SELECT 
  vendor_name AS vendor_id
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
  ,payment_type AS payment_type_id
  ,NULL AS trip_type
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM yellow_tripdata0

UNION ALL

SELECT 
  vendor_id
  ,tpep_pickup_datetime AS pickup_datetime
  ,tpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,pickup_longitude
  ,pickup_latitude
  ,dropoff_longitude
  ,dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,NULL AS ehail_fee
  ,improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM yellow_tripdata1

UNION ALL

SELECT 
  vendor_id
  ,tpep_pickup_datetime AS pickup_datetime
  ,tpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,NULL AS pickup_longitude
  ,NULL AS pickup_latitude
  ,NULL AS dropoff_longitude
  ,NULL AS dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,NULL AS ehail_fee
  ,improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,pickup_location_id
  ,dropoff_location_id
FROM yellow_tripdata2

UNION ALL

SELECT
  base_code AS vendor_id
  ,pickup_datetime
  ,NULL AS dropoff_datetime
  ,NULL AS store_and_fwd_flag
  ,NULL AS rate_code_id
  ,pickup_longitude
  ,pickup_latitude
  ,NULL AS dropoff_longitude
  ,NULL AS dropoff_latitude
  ,NULL AS passenger_count  
  ,NULL AS trip_distance  
  ,NULL AS fare_amount  
  ,NULL AS extra  
  ,NULL AS mta_tax  
  ,NULL AS tip_amount  
  ,NULL AS tolls_amount  
  ,NULL AS ehail_fee  
  ,NULL AS improvement_surcharge  
  ,NULL AS total_amount  
  ,NULL AS payment_type_id  
  ,NULL AS trip_type_id  
  ,NULL AS pickup_location_id  
  ,NULL AS dropoff_location_id  
FROM uber_tripdata0

UNION ALL

SELECT
  dispatching_base_num AS vendor_id
  ,pickup_datetime
  ,NULL AS dropoff_datetime
  ,NULL AS store_and_fwd_flag
  ,NULL AS rate_code_id
  ,NULL AS pickup_longitude
  ,NULL AS pickup_latitude
  ,NULL AS dropoff_longitude
  ,NULL AS dropoff_latitude
  ,NULL AS passenger_count  
  ,NULL AS trip_distance  
  ,NULL AS fare_amount  
  ,NULL AS extra  
  ,NULL AS mta_tax  
  ,NULL AS tip_amount  
  ,NULL AS tolls_amount  
  ,NULL AS ehail_fee  
  ,NULL AS improvement_surcharge  
  ,NULL AS total_amount  
  ,NULL AS payment_type_id  
  ,NULL AS trip_type_id  
  ,location_id AS pickup_location_id  
  ,NULL AS dropoff_location_id  
FROM uber_tripdata1