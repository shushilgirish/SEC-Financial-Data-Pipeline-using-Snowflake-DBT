WITH transformed_data AS (
    SELECT 
        adsh AS filing_id,
        
        -- Create JSON object for company information
        OBJECT_CONSTRUCT(
            'company_name', company_name,
            'cik', cik,
            'sic', sic
        ) AS company_info,

        -- Create JSON array for financial data
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'tag', tag,
                'tag_label', tag_label,
                'tag_description', tag_description,
                'value', numeric_value,
                'unit_of_measure', unit_of_measure,
                'period_end_date', period_end_date,
                'quarters_duration', quarters_duration,
                'statement_type', statement_type,
                'presentation_label', presentation_label
            )
        ) AS financial_data,

        filing_date, 
        fiscal_year, 
        fiscal_period

    FROM {{ ref('stg_financial_data') }}
    GROUP BY adsh, cik, company_name, sic, filing_date, fiscal_year, fiscal_period
)
SELECT * FROM transformed_data
