    -- dbt_project/models/marts/kpi_financial_ratios.sql
-- Étape 1: Importer les modèles de staging déjà nettoyés
with income_statement as (
    select
        *
    from
        { { ref('stg_income_statement') } }
),
balance_sheet as (
    select
        *
    from
        { { ref('stg_balance_sheet') } }
),
-- Étape 2: Joindre les deux tables par année et par entreprise pour avoir une vue complète
final as (
    select
        i.year,
        i.company,
        i.revenues,
        i.gross_profit,
        i.net_income,
        b.current_assets,
        b.current_liabilities,
        b.equity,
        b.total_assets
    from
        income_statement i
        left join balance_sheet b on i.year = b.year
        and i.company = b.company
) -- Étape 3: Calculer les ratios financiers à partir des données jointes
select
    year,
    company,
    -- Ratios de Rentabilité (%)
    safe_divide(net_income, revenues) as net_profit_margin,
    safe_divide(gross_profit, revenues) as gross_profit_margin,
    -- Ratios de Liquidité
    safe_divide(current_assets, current_liabilities) as current_ratio,
    -- Ratios de Rendement (%)
    safe_divide(net_income, equity) as return_on_equity_roe,
    safe_divide(net_income, total_assets) as return_on_assets_roa
from
    final
order by
    year desc