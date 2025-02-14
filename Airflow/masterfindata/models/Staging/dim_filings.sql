{{ config(schema='rdbms_schema',
materialized='table'
) }}

SELECT DISTINCT
    {{
        dbt_utils.generate_surrogate_key([
            'stg_tag.TAG',
            'stg_tag.VERSION',
            'stg_pre.STMT',
            'stg_num.UOM',
            'stg_sub.FILED'
        ])
    }} AS FILINGS_SK,
    stg_tag.TAG AS TAG,
    stg_tag.VERSION AS VERSION,
    COALESCE(stg_tag.DOC, 'Unknown') AS DOC,
    stg_pre.STMT AS StatementType,
    stg_sub.FILED AS FiledDate,
    stg_num.UOM AS UnitOfMeasure -- Example column from another table
FROM
    {{ ref('stg_pre') }} AS stg_pre
JOIN
    {{ ref('stg_tag') }} AS stg_tag
    ON stg_pre.VERSION_TAG = stg_tag.VERSION_TAG
JOIN
    {{ ref('stg_num') }} AS stg_num
    ON stg_num.VERSION_TAG = stg_tag.VERSION_TAG  
JOIN
    {{ ref('stg_sub') }} AS stg_sub  -- Assuming `stg_sub` is needed, else remove this line
    ON stg_sub.ADSH = stg_pre.ADSH
ORDER BY
    stg_tag.TAG
