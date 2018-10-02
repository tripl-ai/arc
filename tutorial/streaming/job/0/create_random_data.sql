-- this is using a custom UDF RANDOM() as the internal function RAND is optimised 
-- in a way which produces same result each execution
-- fields are deliberately converted to STRING to demonstrate typing in next stage
SELECT
  CAST(CAST(RANDOM()*10000000 AS INTEGER) AS STRING) AS customer_id
  ,CAST(CAST(RANDOM()*20000000 AS INTEGER) AS STRING) AS account_number
  ,CAST(ROUND(RANDOM()*50000,2) AS STRING) AS balance
  ,CAST(CAST(RANDOM()*10 AS INTEGER) AS STRING) AS credit_score
FROM stream