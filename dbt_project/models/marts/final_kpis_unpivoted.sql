{ { config(
    materialized = 'incremental',
    unique_key = 'unique_kpi_id'
) } } with ratios as (
    select
        i.company_name,
        i.report_date,
        safe_divide(i.net_income, i.revenues) as net_profit_margin,
        safe_divide(i.gross_profit, i.revenues) as gross_profit_margin,
        safe_divide(b.current_assets, b.current_liabilities) as current_ratio,
        safe_divide(i.net_income, b.equity) as return_on_equity_roe
    from
        { { ref('stg_income_statement') } } i
        join { { ref('stg_balance_sheet') } } b on i.report_date = b.report_date
        and i.company_name = b.company_name
),
unpivoted as (
    select
        company_name,
        report_date,
        'Net Profit Margin' as kpi_name,
        net_profit_margin as kpi_value,
        'Profitability' as kpi_category,
        '%' as kpi_unit
    from
        ratios
    union
    all
    select
        company_name,
        report_date,
        'Gross Profit Margin' as kpi_name,
        gross_profit_margin as kpi_value,
        'Profitability' as kpi_category,
        '%' as kpi_unit
    from
        ratios
    union
    all
    select
        company_name,
        report_date,
        'Current Ratio' as kpi_name,
        current_ratio as kpi_value,
        'Liquidity' as kpi_category,
        'ratio' as kpi_unit
    from
        ratios
    union
    all
    select
        company_name,
        report_date,
        'Return on Equity (ROE)' as kpi_name,
        return_on_equity_roe as kpi_value,
        'Return' as kpi_category,
        '%' as kpi_unit
    from
        ratios
)
select
    *,
    case
        when kpi_unit = '%' then round(kpi_value * 100, 2)
        else round(kpi_value, 2)
    end as kpi_value_adjusted,
    current_timestamp() as calculation_ts,
    farm_fingerprint(
        concat(
            cast(report_date as string),
            company_name,
            kpi_name
        )
    ) as unique_kpi_id
from
    unpivoted
where
    kpi_value is not null { % if is_incremental() % }
    and report_date > (
        select
            max(report_date)
        from
            { { this } }
    ) { % endif % }