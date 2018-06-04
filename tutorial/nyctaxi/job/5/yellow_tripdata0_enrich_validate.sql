SELECT
  SUM(null_vendor_id) = 0 AND SUM(null_payment_type_id) = 0
  ,TO_JSON(
    NAMED_STRUCT(
      'null_vendor_id'
      ,SUM(null_vendor_id)
      ,'null_vendor_name'
      ,COLLECT_LIST(DISTINCT null_vendor_name)
      ,'null_payment_type_id'
      ,SUM(null_payment_type_id)
      ,'null_payment_type'
      ,COLLECT_LIST(DISTINCT null_payment_type)
    )
  )
FROM (
  SELECT 
    CASE WHEN vendor_id IS NULL THEN 1 ELSE 0 END AS null_vendor_id
    ,CASE WHEN vendor_id IS NULL THEN vendor_name ELSE NULL END AS null_vendor_name
    ,CASE WHEN payment_type_id IS NULL THEN 1 ELSE 0 END AS null_payment_type_id
    ,CASE WHEN payment_type_id IS NULL THEN payment_type ELSE NULL END AS null_payment_type
  FROM yellow_tripdata0_enriched
) valid