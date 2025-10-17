select
    company,
    cast(year as int64) as year,
    cast(replace(CURRENT_ASSETS, ',', '.') as float64) as current_assets,
    cast(replace(NON_CURRENT_ASSETS, ',', '.') as float64) as non_current_assets,
    cast(replace(TOTAL_ASSETS, ',', '.') as float64) as total_assets,
    cast(replace(EQUITY, ',', '.') as float64) as equity,
    cast(
        replace(CURRENT_LIABILITIES, ',', '.') as float64
    ) as current_liabilities,
    cast(
        replace(NON_CURRENT_LIABILITIES, ',', '.') as float64
    ) as non_current_liabilities,
    cast(replace(TOTAL_LIABILITIES, ',', '.') as float64) as total_liabilities
from
    { { source('gcs_raw_data', 'raw_balance_sheets') } }