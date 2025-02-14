select 
    ADSH,
    STMT,
    TAG,
    VERSION,
    COALESCE(PLABEL, 'not known') AS PLABEL,
    CONCAT(VERSION, '-', TAG) AS VERSION_TAG
FROM {{ source('PRE', 'RAW_PRE') }}