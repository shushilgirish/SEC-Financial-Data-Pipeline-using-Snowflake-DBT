{{ config(schema='rdbms_schema',
materialized='view'
) }}
select 
    ADSH,
    TAG,
    VERSION,
    DDATE,
    QTRS,
    UOM,
    COALESCE(VALUE, 0) AS VALUE,
    CONCAT(VERSION, '-', TAG) AS VERSION_TAG
from
    {{ source('NUM', 'RAW_NUM') }}

        