-- ensure that records with at least one typing error are below a specified percentage threshold
SELECT 
    (SUM(errors) / COUNT(errors)) < ${record_error_tolerance_percentage}
    ,TO_JSON(NAMED_STRUCT('error', SUM(errors)/ COUNT(errors), 'threshold', ${record_error_tolerance_percentage})) 
FROM (
    SELECT 
        CASE WHEN SIZE(_errors) > 0 THEN 1 ELSE 0 END AS errors
    FROM detail
) valid