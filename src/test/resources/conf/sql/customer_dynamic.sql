-- example of a basic sql transformation
SELECT 
    *
    ,CONCAT(first_name, ' ', last_name) AS full_name
    ,CAST('${current_date}' AS DATE) AS _date
    ,CAST('${current_timestamp}' AS TIMESTAMP) AS _ts
FROM customer