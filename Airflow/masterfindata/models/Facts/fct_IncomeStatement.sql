{{ config(
    materialized='table',
    transient=true,
    query_tag='IS_FACT_BUILD',
    cluster_by=['COMPANY_SK']
) }}

/*+ PARALLEL(8) */
WITH source_filtered AS (
    SELECT /*+ MATERIALIZED */
        stg_num.Value AS VALUE,
        stg_num.ADSH,
        stg_sub.CIK,
        stg_sub.FILED AS FiledDate,
        stg_pre.STMT
    FROM
        {{ ref('stg_num') }} stg_num
    INNER JOIN
        {{ ref('stg_pre') }} stg_pre  
        ON stg_num.ADSH = stg_pre.ADSH
        AND stg_num.TAG = stg_pre.TAG
    WHERE
        stg_pre.STMT = 'IS'
),
source_with_sub AS (
    SELECT /*+ BROADCAST(stg_sub) */
        s.VALUE,
        s.ADSH,
        s.CIK,
        s.FiledDate,
        s.STMT
    FROM
        source_filtered s
    INNER JOIN
        {{ ref('stg_sub') }} stg_sub
        ON s.ADSH = stg_sub.ADSH
    LIMIT 100000 -- Limit for initial testing, remove for production
),
key_data AS (
    SELECT /*+ MERGE */
        src.VALUE,
        src.ADSH AS ADSH_KEY,
        dim_company.COMPANY_SK,
        dim_filings.FILINGS_SK,
        dim_date.date_sk
    FROM
        source_with_sub src
    LEFT JOIN {{ ref('dim_company') }} dim_company
        ON src.CIK = dim_company.CIK
    LEFT JOIN {{ ref('dim_filings') }} dim_filings
        ON src.STMT = dim_filings.StatementType
        AND src.FiledDate = dim_filings.FiledDate
    LEFT JOIN {{ ref('dim_date') }} dim_date
        ON TRY_TO_DATE(src.FiledDate::VARCHAR, 'YYYY-MM-DD') = dim_date.Full_DT
    WHERE 
        dim_company.COMPANY_SK IS NOT NULL
        AND dim_filings.FILINGS_SK IS NOT NULL
)

SELECT 
    ROUND(SUM(k.VALUE), 2) AS FCT_VALUE, 
    dc.COMPANY_NAME, 
    df.FILEDDATE, 
    df.STATEMENTTYPE,
    df.TAG, 
    df.UNITOFMEASURE, 
    df.VERSION
FROM key_data k
INNER JOIN {{ ref('dim_company') }} dc 
    ON k.COMPANY_SK = dc.COMPANY_SK
INNER JOIN {{ ref('dim_filings') }} df 
    ON k.FILINGS_SK = df.FILINGS_SK
GROUP BY dc.COMPANY_NAME, df.FILEDDATE, df.STATEMENTTYPE, df.TAG, df.UNITOFMEASURE, df.VERSION
