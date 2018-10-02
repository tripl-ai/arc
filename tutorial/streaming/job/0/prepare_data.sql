-- this is to create the value column required for tensorflowservingtransform
SELECT
  *
  ,credit_score AS value
FROM random_customer_typed