-- example of a basic sql transformation
SELECT 
    *
    ,CONCAT(first_name, ' ', last_name) AS full_name
FROM customer