import pandas as pd
import numpy as np
import random

# --- 1. Income Statement Generator ---
def generate_income_statement(entreprise, annee):
    
    revenues = np.random.normal(1200000, 150000)
    cogs = revenues * np.random.normal(0.55, 0.05) 
    gross_profit = revenues - cogs
    
    sga = revenues * np.random.normal(0.25, 0.03) 
    depreciation_amortization = np.random.normal(40000, 5000)
    
    operating_income_ebit = gross_profit - sga - depreciation_amortization
    
    interest_expense = np.random.normal(10000, 2000)
    income_before_tax = operating_income_ebit - interest_expense
    
    income_tax = max(0, income_before_tax * np.random.normal(0.25, 0.02))
    net_income = income_before_tax - income_tax
    
    return {
        'Company': entreprise,
        'Year': annee,
        'Revenues': revenues,
        'Cost of Goods Sold (COGS)': cogs,
        'GROSS PROFIT': gross_profit,
        'Selling, General & Administrative (SG&A)': sga,
        'Depreciation & Amortization': depreciation_amortization,
        'OPERATING INCOME (EBIT)': operating_income_ebit,
        'Interest Expense': interest_expense,
        'INCOME BEFORE TAX': income_before_tax,
        'Income Tax': income_tax,
        'NET INCOME': net_income
    }

# --- 2. Balance Sheet Generator ---
def generate_balance_sheet(entreprise, annee, net_income_from_cdr):
    
    # --- Assets ---
    ppe = np.random.normal(350000, 40000)
    intangible_assets = np.random.normal(50000, 10000)
    other_non_current_assets = np.random.normal(15000, 3000)
    non_current_assets = ppe + intangible_assets + other_non_current_assets
    
    inventories = np.random.normal(250000, 30000) 
    trade_receivables = np.random.normal(40000, 10000) 
    cash_equivalents = np.random.normal(120000, 25000)
    other_current_assets = np.random.normal(10000, 2000)
    current_assets = inventories + trade_receivables + cash_equivalents + other_current_assets
    
    total_assets = non_current_assets + current_assets
    
    # --- Liabilities & Equity ---
    long_term_debt = np.random.normal(200000, 30000)
    non_current_liabilities = long_term_debt
    
    trade_payables = np.random.normal(130000, 20000) 
    other_current_liabilities = np.random.normal(30000, 5000)
    current_liabilities = trade_payables + other_current_liabilities
    
    total_liabilities = non_current_liabilities + current_liabilities
    
    share_capital = 100000
    
    net_income_bs = net_income_from_cdr
    
    retained_earnings = total_assets - total_liabilities - share_capital - net_income_bs
    
    equity = share_capital + retained_earnings + net_income_bs
    total_equity_liabilities = equity + total_liabilities
    
    return {
        'Company': entreprise,
        'Year': annee,
        'Property, plant and equipment': ppe,
        'Intangible assets': intangible_assets,
        'Other non-current assets': other_non_current_assets,
        'NON-CURRENT ASSETS': non_current_assets,
        'Inventories': inventories,
        'Trade receivables': trade_receivables,
        'Cash and cash equivalents': cash_equivalents,
        'Other current assets': other_current_assets,
        'CURRENT ASSETS': current_assets,
        'TOTAL ASSETS': total_assets,
        'Share capital': share_capital,
        'Retained earnings': retained_earnings,
        'Net income': net_income_bs,
        'EQUITY': equity,
        'Long-term debt': long_term_debt,
        'NON-CURRENT LIABILITIES': non_current_liabilities,
        'Trade payables': trade_payables,
        'Other current liabilities': other_current_liabilities,
        'CURRENT LIABILITIES': current_liabilities,
        'TOTAL LIABILITIES': total_liabilities,
        'TOTAL EQUITY AND LIABILITIES': total_equity_liabilities
    }

# --- 3. Génération des Données ---
entreprises = ['SuperRetail A']
annees = [2024]

cdr_data = []
bilan_data = []

for entreprise in entreprises:
    for annee in annees:
        # 1. Générer le compte de résultat et le sauvegarder
        income_statement = generate_income_statement(entreprise, annee)
        cdr_data.append(income_statement)
        
        # 2. Extraire le résultat net calculé
        net_income_value = income_statement['NET INCOME']
        
        # 3. Générer le bilan en lui passant le résultat net
        balance_sheet = generate_balance_sheet(entreprise, annee, net_income_value)
        bilan_data.append(balance_sheet)

# --- 4. Création et Sauvegarde des DataFrames ---
cdr_df = pd.DataFrame(cdr_data).round(0)
cdr_cols = [
    'Company', 'Year', 'Revenues', 'Cost of Goods Sold (COGS)', 'GROSS PROFIT',
    'Selling, General & Administrative (SG&A)', 'Depreciation & Amortization',
    'OPERATING INCOME (EBIT)', 'Interest Expense', 'INCOME BEFORE TAX',
    'Income Tax', 'NET INCOME'
]
cdr_df = cdr_df[cdr_cols]

bilan_df = pd.DataFrame(bilan_data).round(0)
bilan_cols = [
    'Company', 'Year',
    'Property, plant and equipment', 'Intangible assets', 'Other non-current assets',
    'NON-CURRENT ASSETS', 'Inventories', 'Trade receivables', 
    'Cash and cash equivalents', 'Other current assets', 'CURRENT ASSETS', 'TOTAL ASSETS',
    'Share capital', 'Retained earnings', 'Net income', 'EQUITY',
    'Long-term debt', 'NON-CURRENT LIABILITIES', 'Trade payables',
    'Other current liabilities', 'CURRENT LIABILITIES', 'TOTAL LIABILITIES',
    'TOTAL EQUITY AND LIABILITIES'
]
bilan_df = bilan_df[bilan_cols]

cdr_df.to_csv('income_statement.csv', index=False, sep=';', decimal=',')
bilan_df.to_csv('balance_sheet.csv', index=False, sep=';', decimal=',')

print("--- Income Statement (Aperçu) ---")
print(cdr_df.head())
print("\n--- Balance Sheet (Aperçu) ---")
print(bilan_df.head())
print("\nFichiers CSV 'income_statement.csv' et 'balance_sheet.csv' générés avec succès.")c