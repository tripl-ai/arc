-- ensure that records with at least one typing error are below 5%
SELECT 
    (SUM(errors) / COUNT(errors)) < 0.05
    ,TO_JSON(NAMED_STRUCT('error', SUM(errors)/ COUNT(errors), 'threshold', 0.05)) 
FROM (
    SELECT 
        CASE WHEN SIZE(_errors) > 0 THEN 1 ELSE 0 END AS errors
    FROM detail
) valid