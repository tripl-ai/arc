-- only select columns which job is authorised to access based on ${pii_authorized}
SELECT 
    name 
FROM metadata 
WHERE metadata.pii = (
    CASE 
        WHEN ${pii_authorized} = true 
        THEN metadata.pii   -- this will allow both true and false metadata.pii values if pii_authorized = true
        ELSE false          -- else if pii_authorized = false only allow metadata.pii = false values
    END
)