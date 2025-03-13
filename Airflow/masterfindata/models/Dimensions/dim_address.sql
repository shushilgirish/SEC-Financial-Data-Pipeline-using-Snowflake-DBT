{{ config(
    materialized='view',
    schema='rdbms_schema'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'BAS1', 
        'BAS2', 
        'STPRBA', 
        'COUNTRYBA', 
        'ZIPBA'
    ]) }} AS COMP_ADDRESS_SK, 
    Name AS Company_Name,
    BAS1 AS Street_Address1,
    BAS2 AS Street_Address2,
    STPRBA AS State_or_Province,
    COUNTRYBA AS Country,
    ZIPBA AS Zipcode
FROM 
    {{ ref('stg_sub') }}
