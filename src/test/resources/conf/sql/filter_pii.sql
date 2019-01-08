-- only select columns which job is authorised to access
SELECT 
    name 
FROM metadata 
WHERE metadata.pii = false