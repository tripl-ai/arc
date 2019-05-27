SELECT
  SUM(error) = 0 AS valid
  ,TO_JSON(NAMED_STRUCT('count', COUNT(error), 'errors', SUM(error))) AS message
FROM (
  SELECT 
    CASE 
      WHEN SIZE(_errors) > 0 THEN 1 
      ELSE 0 
    END AS error 
  FROM ${table_name}
) input_table