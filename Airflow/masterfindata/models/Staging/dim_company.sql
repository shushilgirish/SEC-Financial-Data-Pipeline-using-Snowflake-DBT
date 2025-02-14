select DISTINCT
    {{
        dbt_utils.generate_surrogate_key([
            'Cik',
            'Company_Name'
        ])
    }} as Company_SK,
	STG_SUB.CIK as CIK,
	STG_SUB.Name as Company_Name,
	UPPER(SPLIT_PART(Instance, '-', 1)) AS Ticker,
	dim_address.COMP_ADDRESS_SK as COMP_ADDRESS_SK,
from
    {{ ref('dim_address') }} as dim_address
join
    {{ ref('stg_sub') }} as STG_SUB
        on dim_address.Company_Name = STG_SUB.Name
order by
    STG_SUB.Name

