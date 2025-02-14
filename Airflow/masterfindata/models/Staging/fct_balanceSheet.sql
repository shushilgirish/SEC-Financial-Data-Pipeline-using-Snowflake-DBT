{{ config(schema='rdbms_schema',
materialized='table'
) }}

WITH source AS (
    SELECT 
        stg_num.Value AS VALUE,
        stg_num.ADSH AS ADSH,
        stg_sub.CIK, 
        stg_sub.FILED AS FiledDate,
        stg_sub.INSTANCE,
        stg_pre.STMT
    FROM 
        {{ ref('stg_num') }} stg_num
    INNER JOIN 
        {{ ref('stg_sub') }} stg_sub 
        ON stg_num.ADSH = stg_sub.ADSH
    INNER JOIN 
        {{ ref('stg_pre') }} stg_pre  
        ON stg_num.ADSH = stg_pre.ADSH 
        AND stg_num.TAG = stg_pre.TAG  -- Ensuring correct financial statement mapping
    WHERE 
        stg_pre.STMT = 'BS'  -- Filter only Balance Sheet Data
),

dedup_source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ADSH, VALUE ORDER BY FiledDate DESC) AS row_num
    FROM source
),

final AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['dim_company.COMPANY_SK','dim_filings.FILINGS_SK','ds.FiledDate','ds.ADSH', 'ds.VALUE']) }} AS COMP_FCT_SK,  -- Fact Table SK
        dim_company.COMPANY_SK,
        dim_filings.FILINGS_SK,
        ds.FiledDate, 
        ds.VALUE,
        ds.ADSH AS ADSH_KEY,
        'DBT_USER' AS Created_By,  -- Placeholder for tracking
        'BS' AS STMT,  -- Marking as Balance Sheet Data
        CURRENT_TIMESTAMP() AS Created_DT
    FROM 
        dedup_source ds
    LEFT JOIN {{ ref('dim_company') }} dim_company 
        ON ds.CIK = dim_company.CIK
    LEFT JOIN {{ ref('dim_filings') }} dim_filings 
        ON ds.STMT = dim_filings.StatementType  
        AND ds.FiledDate = dim_filings.FiledDate  -- Fixed ambiguity by explicitly using `ds.FiledDate`
    LEFT JOIN {{ ref('dim_date') }} dim_date 
        ON TRY_TO_DATE(ds.FiledDate::STRING, 'YYYY-MM-DD') = dim_date.Full_DT  -- Explicit reference to `ds.FiledDate`
    WHERE ds.row_num = 1  -- Keeping only the latest record per ADSH & VALUE
)

SELECT * FROM final
