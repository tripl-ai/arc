-- this is using a custom UDF RANDOM() as the internal function RAND is optimised 
-- in a way which produces same result each execution
SELECT
  CAST(RANDOM()*10000000 AS INT) AS customer_id
  ,CAST(RANDOM()*20000000 AS INT) AS account_number
  ,ROUND(RANDOM()*50000,2) AS balance
  ,CAST(RANDOM()*10 AS INT) AS value
FROM stream