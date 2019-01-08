SELECT
  SUM(error) = 0
  ,TO_JSON(NAMED_STRUCT('count', COUNT(error), 'errors', SUM(error)))
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN ${test_integer}
      ELSE 0
    END AS error
  FROM ${table_name}
) input_table