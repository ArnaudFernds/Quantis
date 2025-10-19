import pandas as pd

def compute_kpis(df_bilan: pd.DataFrame, df_cdr: pd.DataFrame) -> pd.DataFrame:
    for col in ["company", "period"]:
        if col not in df_bilan.columns:
            df_bilan[col] = None
        if col not in df_cdr.columns:
            df_cdr[col] = None

    bilan_cols = ["assets","equity","debts","cash","current_assets","current_liabilities"]
    cdr_cols = ["revenue","expenses","ebit","net_income"]

    for c in bilan_cols:
        if c not in df_bilan.columns:
            df_bilan[c] = 0.0
    for c in cdr_cols:
        if c not in df_cdr.columns:
            df_cdr[c] = 0.0

    df = pd.merge(
        df_cdr[["company","period"] + cdr_cols],
        df_bilan[["company","period"] + bilan_cols],
        on=["company","period"],
        how="outer"
    ).fillna(0)

    df["margin"] = (df["revenue"] - df["expenses"]) / df["revenue"].replace({0: pd.NA})
    df["roe"] = df["net_income"] / df["equity"].replace({0: pd.NA})
    df["roa"] = df["net_income"] / df["assets"].replace({0: pd.NA})
    df["working_capital"] = df["current_assets"] - df["current_liabilities"]
    df["bfr"] = df["working_capital"]
    df["leverage_ratio"] = df["debts"] / df["equity"].replace({0: pd.NA})
    df["profitability"] = df["ebit"] / df["assets"].replace({0: pd.NA})

    return df
