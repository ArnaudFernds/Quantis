import pandas as pd

def _cleanup(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    return df

def _normalize_any(df: pd.DataFrame) -> pd.DataFrame:
    """
    Robustifier:
    - Cas 2 colonnes: (metric,value) si la 1ère ressemble à 'metric'/'name'/... sinon fallback
    - Cas 1 ligne n colonnes: on prend la 1ère ligne comme valeurs et on melt en (metric,value)
    """
    df = _cleanup(df)
    if df.empty:
        return pd.DataFrame(columns=["metric", "value"])

    lower_cols = [c.lower() for c in df.columns]
    # Cas (metric,value)
    if len(df.columns) == 2 and lower_cols[0] in ("metric","kpi","name","libelle","label"):
        out = df.rename(columns={df.columns[0]:"metric", df.columns[1]:"value"})[["metric","value"]]
    else:
        # on prend la 1ère ligne comme séries de valeurs, colonnes = noms de métriques
        out = df.iloc[0:1].melt(var_name="metric", value_name="value")

    out["metric"] = out["metric"].astype(str).str.strip().str.lower()
    out["value"]  = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["value"])
    return out

def compute_kpis_from_frames(bilan_df: pd.DataFrame, cdr_df: pd.DataFrame) -> pd.DataFrame:
    b = _normalize_any(bilan_df)
    c = _normalize_any(cdr_df)
    # On concatène tout simplement pour un premier chargement
    return pd.concat([b, c], ignore_index=True)
