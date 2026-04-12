import os
import logging
import pandas as pd
from datetime import datetime


# --- CONFIG ---
INPUT_PATH = "employee.csv"
OUTPUT_PATH = "clean_employee.csv"

REQUIRED_COLUMNS = ["id", "name", "age", "join_date", "salary", "dept"]

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")


# --- STEP 1: LOAD ---
def load_csv(fp) -> pd.DataFrame:
    if not os.path.exists(fp):
        raise FileNotFoundError(f"File not found: {fp}")

    df = pd.read_csv(fp)
    if df.empty:
        raise ValueError("Input file is empty")

    logging.info(f"Loaded {len(df)} rows")
    return df


# --- STEP 2: NORMALIZE COLUMNS ---
def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df, list(df.columns)


# --- STEP 3: VALIDATE COLUMNS ---
def validate_columns(columns: list[str]) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    extra = [c for c in columns if c not in REQUIRED_COLUMNS]
    if extra:
        logging.warning(f"Unexpected columns present: {extra}")

    logging.info("Column validation passed")


# --- STEP 4: TRANSFORM DATA ---
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    initial_rows = len(df)

    # ID
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.dropna(subset=["id"])
    df["id"] = df["id"].astype(int)

    # NAME
    df["name"] = df["name"].astype(str).str.strip()
    df.loc[df["name"] == "", "name"] = None

    # AGE
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    # JOIN DATE
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")
    df["join_date"] = df["join_date"].dt.strftime("%Y-%m-%d")

    # SALARY
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

    # DEPT
    df["dept"] = df["dept"].astype(str).str.strip()
    df.loc[df["dept"] == "", "dept"] = None

    final_rows = len(df)
    dropped_rows = initial_rows - final_rows

    logging.info(f"Rows after cleaning: {final_rows}")
    logging.info(f"Rows dropped: {dropped_rows}")

    return df


# --- STEP 5: SAVE ---
def save_csv(df: pd.DataFrame, fp) -> None:
    df = df[REQUIRED_COLUMNS]
    df.to_csv(fp, na_rep="", index=False)
    logging.info(f"Output file written to {fp}")


# --- PIPELINE ---
def run_pipeline() -> None:
    raw_df = load_csv(INPUT_PATH)
    norm_df, columns = normalize_columns(raw_df)
    validate_columns(columns)
    clean_df = transform_data(norm_df)
    save_csv(clean_df, OUTPUT_PATH)


# --- ENTRY POINT ---
if __name__ == "__main__":
    try:
        run_pipeline()
        print("SUCCESS")
    except Exception:
        logging.exception("Pipeline failed")
        print("FAILED")
