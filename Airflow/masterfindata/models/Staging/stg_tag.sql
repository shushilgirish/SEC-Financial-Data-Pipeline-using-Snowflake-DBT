{{ config(schema='rdbms_schema',
materialized='view'
) }}

select
    TAG,
    VERSION,
    COALESCE(TLABEL, 'not known') AS TLABEL,
    DOC,
    CONCAT(VERSION, '-', TAG) AS VERSION_TAG
FROM {{ source('TAG', 'RAW_TAG') }}