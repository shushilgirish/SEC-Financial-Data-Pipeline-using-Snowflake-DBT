-- models/dim_date.sql
{{ config(materialized='table') }}
 
WITH calendar AS (
    SELECT
      DATEADD(day, row_number() OVER (ORDER BY NULL) - 1, '2000-01-01') AS full_dt
    FROM TABLE(GENERATOR(ROWCOUNT => 11323))
),
date_dim AS (
    SELECT
      TO_NUMBER(TO_CHAR(full_dt, 'YYYYMMDD')) AS date_sk,
      full_dt,
      EXTRACT(YEAR   FROM full_dt)   AS year,
      EXTRACT(MONTH  FROM full_dt)   AS month,
      CASE
         WHEN EXTRACT(MONTH FROM full_dt) BETWEEN 1 AND 3 THEN 1
         WHEN EXTRACT(MONTH FROM full_dt) BETWEEN 4 AND 6 THEN 2
         WHEN EXTRACT(MONTH FROM full_dt) BETWEEN 7 AND 9 THEN 3
         ELSE 4
      END AS quarter,
      EXTRACT(DAY    FROM full_dt)   AS day_of_month,
      EXTRACT(DOW    FROM full_dt)   AS day_of_week,
      CASE
         WHEN EXTRACT(DOW FROM full_dt) IN (6,7) THEN 'Y'
         ELSE 'N'
      END AS is_weekend
    FROM calendar
)
 
SELECT *
FROM date_dim