import pandas as pd
from sqlalchemy import create_engine
import json

# -----------------------------
# Load config
# -----------------------------
with open("config.json") as f:
    config = json.load(f)

# -----------------------------
# Load data from Google Sheets
# -----------------------------
url = config["google_sheet_url"]
df = pd.read_csv(url)

# -----------------------------
# CLEAN DATA (very important)
# -----------------------------
df['ok_quantity'] = pd.to_numeric(df['ok_quantity'].astype(str).str.replace(',', ''))

defect_cols = [
    "B-QUALITY","BEADING","CUTTING","DENT","INSIDE SOS",
    "LINING","LOOSE CAP","NIPPLE NOT FITTING","PLAIN BOTTOM",
    "REPLATING","REPOLISH","SCRAP","TIGHT CAP"
]

for col in defect_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['date'] = pd.to_datetime(df['date'], dayfirst=True)

# -----------------------------
# LOAD TO DATABASE
# ------------prostgres db -----------------
# engine = create_engine(config["db_connection"])

# df.to_sql("raw_production", engine, if_exists="replace", index=False)

# print("Data loaded successfully")

# -----------google cloud bigQuery ------------------
from google.cloud import bigquery

# hardcoded credentials -------------------------
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    "C:/Users/Praks 7v/Projects/manufacturing-data-pipeline/ingestion/data-analysis-490514-f5dff2c9139f.json"
)
# -----------------------------------------------
client = bigquery.Client(
    credentials=credentials,
    project="data-analysis-490514"
)

table_id = "data-analysis-490514.manufacturing_pipeline.raw_production"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print("Data loaded into BigQuery successfully")