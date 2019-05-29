-- example of a sql statement with basic cte
WITH customer_contact AS (
    SELECT
        *,
        CONCAT(first_name, ' ', last_name) AS full_name
    FROM
        customer
)
SELECT
    full_name
FROM
    customer_contact