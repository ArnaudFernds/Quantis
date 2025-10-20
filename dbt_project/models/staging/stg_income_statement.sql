select
    company as company_name,
    parse_date('%Y', cast(year as string)) as report_date,
    cast(replace(revenues, ',', '.') as float64) as revenues,
    cast(replace(GROSS_PROFIT, ',', '.') as float64) as gross_profit,
    cast(replace(NET_INCOME, ',', '.') as float64) as net_income
from
    { { source('gcs_raw_data', 'raw_income_statements') } }