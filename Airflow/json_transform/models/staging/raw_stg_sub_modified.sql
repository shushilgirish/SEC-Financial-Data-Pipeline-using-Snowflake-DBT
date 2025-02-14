SELECT 
    adsh,  -- Accession Number (Primary Key)
    cik,  -- Central Index Key
    name,  -- Registrant Name
    sic,  -- Standard Industrial Classification
    countryba,  -- Business Address Country
    stprba,  -- Business Address State/Province
    cityba,  -- Business Address City
    zipba,  -- Business Address Zip Code
    bas1,  -- Business Address Street Line 1
    bas2,  -- Business Address Street Line 2
    baph,  -- Business Address Phone
    countryma,  -- Mailing Address Country
    stprma,  -- Mailing Address State/Province
    cityma,  -- Mailing Address City
    zipma,  -- Mailing Address Zip Code
    mas1,  -- Mailing Address Street Line 1
    mas2,  -- Mailing Address Street Line 2
    countryinc,  -- Country of Incorporation
    stprinc,  -- State of Incorporation
    ein,  -- Employer Identification Number
    former,  -- Most Recent Former Name
    changed,  -- Date of Change from Former Name
    afs,  -- Filer Status
    wksi,  -- Well Known Seasoned Issuer Indicator
    fye,  -- Fiscal Year End Date
    form,  -- Submission Type (10-K, 10-Q, etc.)
   {{handle_null_date('period')}} AS period,  -- ✅ Replace NULL period with default date
    fy,  -- Fiscal Year
    fp,  -- Fiscal Period
    filed,  -- Filing Date
    accepted,  -- Acceptance Date & Time
    prevrpt,  -- Previous Report Indicator
    detail,  -- Detail Level Indicator
    instance,  -- XBRL Instance Document
    nciks,  -- Number of Central Index Keys
    aciks  -- Additional Co-Registrants
FROM {{ source('json_transformation', 'raw_sub') }}
