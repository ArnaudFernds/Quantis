select
    company,
    cast(year as int64) as year,
    cast(replace(NET_INCOME, ',', '.') as float64) as net_income
from
    {{ source('your_source_name', 'raw_income_statements') }}
select
    company,
    cast(year as int64) as year,
    cast(replace(revenues, ',', '.') as float64) as revenues,
    cast(
        replace(Cost_of_Goods_Sold_COGS, ',', '.') as float64
    ) as cogs,
    cast(replace(GROSS_PROFIT, ',', '.') as float64) as gross_profit,
    cast(
        replace(Selling_General_Administrative_SGA, ',', '.') as float64
    ) as sga_expenses,
    cast(
        replace(Depreciation_Amortization, ',', '.') as float64
    ) as depreciation_amortization,
    cast(
        replace(OPERATING_INCOME_EBIT, ',', '.') as float64
    ) as ebit,
    cast(replace(Interest_Expense, ',', '.') as float64) as interest_expense,
    cast(replace(INCOME_BEFORE_TAX, ',', '.') as float64) as income_before_tax,
    cast(replace(Income_Tax, ',', '.') as float64) as income_tax,
    cast(replace(NET_INCOME, ',', '.') as float64) as net_income
from
    { { source('gcs_raw_data', 'raw_income_statements') } }