select
    company,
    cast(year as int64) as year,
    cast(replace(NET_INCOME, ',', '.') as float64) as net_income
from
    {{ source('your_source_name', 'raw_income_statements') }}