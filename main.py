import os, io, sys, datetime as dt
import pandas as pd
from google.cloud import storage, bigquery

CSV_SEP = ","
CSV_DEC = "."

def latest_csv_uri(bucket_name, prefix=None):
    client = storage.Client()
    blobs = [b for b in client.list_blobs(bucket_name, prefix=prefix) if b.name.lower().endswith(".csv")]
    if not blobs:
        raise RuntimeError(f"Aucun CSV trouvé dans gs://{bucket_name}/{prefix or ''}")
    latest = max(blobs, key=lambda b: b.time_created)
    print(f"[INFO] Fichier retenu gs://{bucket_name}/{latest.name}")
    return f"gs://{bucket_name}/{latest.name}"

def read_csv_gcs(uri: str) -> pd.DataFrame:
    client = storage.Client()
    bucket, key = uri.replace("gs://", "").split("/", 1)
    data = client.bucket(bucket).blob(key).download_as_bytes()
    df = pd.read_csv(io.BytesIO(data), sep=CSV_SEP, decimal=CSV_DEC)
    print(f"[INFO] Lecture OK sep='{CSV_SEP}' decimal='{CSV_DEC}' shape={df.shape}")
    return df

COLUMNS_CDR = {
    'Company':'company','Year':'year','Revenues':'revenues',
    'Cost of Goods Sold (COGS)':'cost_of_goods_sold','GROSS PROFIT':'gross_profit',
    'Selling, General & Administrative (SG&A)':'selling_general_and_administrative',
    'Depreciation & Amortization':'depreciation_and_amortization',
    'OPERATING INCOME (EBIT)':'operating_income','Interest Expense':'interest_expense',
    'INCOME BEFORE TAX':'ibt','Income Tax':'tax','NET INCOME':'net_income',
}
COLUMNS_BS = {
    'Company':'company','Year':'year','Property, plant and equipment':'ppe',
    'Intangible assets':'intangibles','Other non-current assets':'other_nca',
    'NON-CURRENT ASSETS':'non_current_assets','Inventories':'inventories',
    'Trade receivables':'receivables','Cash and cash equivalents':'cash',
    'Other current assets':'other_ca','CURRENT ASSETS':'current_assets',
    'TOTAL ASSETS':'total_assets','Share capital':'share_capital',
    'Retained earnings':'retained_earnings','Net income':'net_income_bs',
    'EQUITY':'equity','Long-term debt':'long_term_debt',
    'NON-CURRENT LIABILITIES':'non_current_liabilities',
    'Trade payables':'payables','Other current liabilities':'other_cl',
    'CURRENT LIABILITIES':'current_liabilities','TOTAL LIABILITIES':'total_liabilities',
    'TOTAL EQUITY AND LIABILITIES':'total_equity_and_liabilities',
}

def rename_like(df, mapping):
    print(f"[INFO] Colonnes avant: {list(df.columns)}")
    df = df.rename(columns=mapping)
    print(f"[INFO] Colonnes après : {list(df.columns)}")
    return df

def kpis_from_frames(cdr: pd.DataFrame, bs: pd.DataFrame) -> pd.DataFrame:
    for req in ["company","year"]:
        if req not in cdr.columns or req not in bs.columns:
            raise KeyError(f"Colonne requise absente pour merge: {req}")
    df = pd.merge(cdr, bs, on=["company","year"], how="inner")

    rows = []
    for _, r in df.iterrows():
        comp = str(r["company"])
        year = int(r["year"])
        report_date = dt.date(year, 12, 31)

        pairs = [
            ("revenue", r.get("revenues")),
            ("gross_margin", (r.get("gross_profit") / r.get("revenues")) if r.get("revenues") not in [0, None] else None),
            ("operating_margin", (r.get("operating_income") / r.get("revenues")) if r.get("revenues") not in [0, None] else None),
            ("equity", r.get("equity")),
            ("total_assets", r.get("total_assets")),
            ("net_income", r.get("net_income")),
            ("long_term_debt", r.get("long_term_debt")),
            ("current_ratio", (r.get("current_assets") / r.get("current_liabilities")) if r.get("current_liabilities") not in [0, None] else None),
            ("debt_to_equity", (r.get("total_liabilities") / r.get("equity")) if r.get("equity") not in [0, None] else None),
        ]
        for name, val in pairs:
            rows.append({
                "company_name": comp,
                "report_date": report_date,
                "kpi_name": name,
                "kpi_value": float(val) if pd.notna(val) else None,
                "kpi_category": "financial",
                "kpi_unit": "EUR" if name in ["revenue","equity","total_assets","net_income","long_term_debt"] else None,
                "calculation_ts": pd.Timestamp.utcnow(),
            })
    return pd.DataFrame(rows)

def normalize_for_bq(out: pd.DataFrame) -> pd.DataFrame:
    # DATE (not datetime64)
    out["report_date"] = pd.to_datetime(out["report_date"], errors="coerce").dt.date

    # Required numeric
    out["kpi_value"] = pd.to_numeric(out["kpi_value"], errors="coerce")
    out = out.dropna(subset=["kpi_value"])

    # Force REQUIRED strings to real Python str (not pandas StringDtype)
    for col in ["company_name", "kpi_name", "kpi_category"]:
        out[col] = out[col].fillna("").astype(object).apply(str)

    # NULLABLE string
    if "kpi_unit" in out.columns:
        out["kpi_unit"] = out["kpi_unit"].astype(object).where(out["kpi_unit"].notna(), None)

    # TIMESTAMP
    if "calculation_ts" not in out.columns or out["calculation_ts"].isna().all():
        out["calculation_ts"] = pd.Timestamp.utcnow()
    else:
        out["calculation_ts"] = pd.to_datetime(out["calculation_ts"], errors="coerce")

    out = out[["company_name","report_date","kpi_name","kpi_value","kpi_category","kpi_unit","calculation_ts"]]
    print(f"[INFO] Lignes prêtes: {len(out)}  Colonnes: {list(out.columns)}")
    return out

def main():
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("PROJECT") or os.environ.get("PROJECT_ID")
    dataset = os.environ["BQ_DATASET"]
    table   = os.environ["BQ_TABLE"]
    bkt_bs  = os.environ["GCS_BUCKET_BILAN"]
    bkt_is  = os.environ["GCS_BUCKET_CDR"]

    print(f"[INFO] Projet={project} Dataset={dataset} Table={table}")
    print(f"[INFO] Buckets: bilan={bkt_bs} cdr={bkt_is}")

    bs_uri  = latest_csv_uri(bkt_bs)
    is_uri  = latest_csv_uri(bkt_is)
    df_cdr  = read_csv_gcs(is_uri)
    df_bilan= read_csv_gcs(bs_uri)
    print(f"[INFO] RAW shapes: bilan={df_bilan.shape} cdr={df_cdr.shape}")

    df_cdr   = rename_like(df_cdr, COLUMNS_CDR)
    df_bilan = rename_like(df_bilan, COLUMNS_BS)

    out = kpis_from_frames(df_cdr, df_bilan)
    out = normalize_for_bq(out)

    if out.empty:
        print("[INFO] Aucun KPI calculé après filtrage -> arrêt.")
        sys.exit(0)

    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False,
        schema=[
            bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("report_date",  "DATE",   mode="REQUIRED"),
            bigquery.SchemaField("kpi_name",     "STRING", mode="REQUIRED"),
            bigquery.SchemaField("kpi_value",    "FLOAT",  mode="REQUIRED"),
            bigquery.SchemaField("kpi_category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("kpi_unit",     "STRING", mode="NULLABLE"),
            bigquery.SchemaField("calculation_ts","TIMESTAMP", mode="REQUIRED"),
        ],
    )

    job = client.load_table_from_dataframe(out, table_id, job_config=job_config)
    job.result()
    print(f"[INFO] Chargé {len(out)} lignes dans {table_id}")

if __name__ == "__main__":
    main()
