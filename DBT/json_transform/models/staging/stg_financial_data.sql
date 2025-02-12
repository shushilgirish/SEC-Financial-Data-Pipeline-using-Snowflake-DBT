WITH sub_data AS (
    SELECT 
        adsh,
        cik,
        filed AS filing_date,
        fy AS fiscal_year,
        fp AS fiscal_period,
        name AS company_name,
        sic,
        countryba,
        stprba,
        cityba
    FROM {{ ref('raw_stg_sub_modified') }}
),
num_data AS (
    SELECT 
        adsh,
        tag,
        version,
        ddate AS period_end_date,
        qtrs AS quarters_duration,
        uom AS unit_of_measure,
        value,
        footnote
    FROM {{ source('json_transformation', 'raw_num') }}
),
tag_data AS (
    SELECT 
        tag,
        version,
        tlabel AS tag_label,
        doc AS tag_description
    FROM {{ source('json_transformation', 'raw_tag') }}
),
pre_data AS (
    SELECT 
        adsh,
        report,
        line,
        stmt AS statement_type,
        tag,
        plabel AS presentation_label
    FROM {{ source('json_transformation', 'raw_pre') }}
)
SELECT 
    s.adsh,
    s.cik,
    s.filing_date,
    s.fiscal_year,
    s.fiscal_period,
    s.company_name,
    s.sic,
    n.tag,
    n.version,
    n.period_end_date,
    n.quarters_duration,
    n.unit_of_measure,
    n.value AS numeric_value,
    n.footnote,
    t.tag_label,
    t.tag_description,
    p.statement_type,
    p.presentation_label
FROM sub_data s
LEFT JOIN num_data n ON s.adsh = n.adsh
LEFT JOIN tag_data t ON n.tag = t.tag AND n.version = t.version
LEFT JOIN pre_data p ON n.adsh = p.adsh AND n.tag = p.tag
