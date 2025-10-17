-- dbt_project/models/staging/stg_balance_sheet.sql
select
    company,
    cast(year as int64) as year,
    -- ... (mêmes transformations que précédemment)
    cast(replace(TOTAL_LIABILITIES, ',', '.') as float64) as total_liabilities
from
    { { source('your_source_name', 'raw_balance_sheets') } }