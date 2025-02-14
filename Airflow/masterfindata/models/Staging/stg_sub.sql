SELECT 
    ADSH,
    CIK,
    NAME,
    COALESCE(COUNTRYBA, 'Unknown') AS COUNTRYBA,
    COALESCE(STPRBA, 'Unknown') AS STPRBA,
    COALESCE(CITYBA, 'Unknown') AS CITYBA,
    COALESCE(ZIPBA, 'Unknown') AS ZIPBA,
    COALESCE(BAS1, 'Unknown') AS BAS1,
    COALESCE(BAS2, 'Does not exist or Unknown') AS BAS2,
    BAPH,
    FILED,
    ACCEPTED,
    INSTANCE,
    UPPER(SPLIT_PART(Instance, '-', 1)) AS Ticker
FROM {{ source('SUB', 'RAW_SUB') }}