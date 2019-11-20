SELECT
  SUM(error) = 0
  ,TO_JSON(NAMED_STRUCT('count', COUNT(error), 'pii_columns', SUM(error)))
FROM (
  SELECT
    CASE
      WHEN metadata.pii = true THEN 1
      ELSE 0
    END AS error
  FROM metadata
) metadata