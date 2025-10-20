select
    company as company_name,
    parse_date('%Y', cast(year as string)) as report_date,
    cast(replace(CURRENT_ASSETS, ',', '.') as float64) as current_assets,
    cast(replace(TOTAL_ASSETS, ',', '.') as float64) as total_assets,
    cast(replace(EQUITY, ',', '.') as float64) as equity,
    cast(
        replace(CURRENT_LIABILITIES, ',', '.') as float64
    ) as current_liabilities
from
    { { source('gcs_raw_data', 'raw_balance_sheets') } }