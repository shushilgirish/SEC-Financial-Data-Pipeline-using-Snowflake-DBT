-- Changed from view to table for better performance
{{ config(
    materialized='view',  
    schema='rdbms_schema',
    cluster_by=['ADSH', 'TAG', 'VERSION_TAG']
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

