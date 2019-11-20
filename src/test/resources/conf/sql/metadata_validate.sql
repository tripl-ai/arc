SELECT
  SUM(error) = 0
  ,TO_JSON(NAMED_STRUCT('count', COUNT(error), 'securityLevelGreaterThan${securityLevel}', SUM(error)))
FROM (
  SELECT
    CASE
      WHEN metadata.securityLevel > ${securityLevel} THEN 1
      ELSE 0
    END AS error
  FROM metadata
) metadata