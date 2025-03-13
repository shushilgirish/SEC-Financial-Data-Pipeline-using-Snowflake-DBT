{{ config(
    materialized='table',  -- Changed from view to table for better performance
    schema='rdbms_schema',
    indexes=[
        {'columns': ['ADSH', 'TAG'], 'type': 'clustered'},
        {'columns': ['VERSION_TAG'], 'type': 'regular'}
    ]
) }}

SELECT 
    ADSH,
    TAG,
    VERSION,
    DDATE,
    QTRS,
    UOM,
    VALUE,
    FOOTNOTE,
    CONCAT(VERSION, '-', TAG) AS VERSION_TAG
FROM 
    DBT_DB.DBT_SCHEMA.RAW_NUM

