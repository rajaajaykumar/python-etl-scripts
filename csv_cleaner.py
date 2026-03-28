import os
import csv
import logging
from datetime import datetime


# --- CONFIG ---
INPUT_PATH = "employee.csv"
OUTPUT_PATH = "clean_employee.csv"

REQUIRED_COLUMNS = ["id", "name", "age", "join_date", "salary", "dept"]

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")


# --- STEP 1: LOAD ---
def load_csv(fp) -> list[dict]:
    if not os.path.exists(fp):
        raise FileNotFoundError(f"File not found: {fp}")

    with open(fp, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        data = list(reader)

    if not data:
        raise ValueError("Input file is empty")

    logging.info(f"Loaded {len(data)} rows")
    return data


# --- STEP 2: NORMALIZE COLUMNS ---
def normalize_columns(data: list[dict]) -> tuple[list[dict], list[str]]:
    column_mapping = {c: c.strip().lower().replace(" ", "_") for c in data[0].keys()}

    normalized_data = []
    for row in data:
        new_row = {column_mapping[k]: v for k, v in row.items()}
        normalized_data.append(new_row)

    return normalized_data, list(column_mapping.values())


# --- STEP 3: VALIDATE COLUMNS ---
def validate_columns(columns: list) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    extra = [c for c in columns if c not in REQUIRED_COLUMNS]
    if extra:
        logging.warning(f"Unexpected columns present: {extra}")

    logging.info("Column validation passed")


# --- STEP 4: TRANSFORM ---
def transform_data(data: list[dict]) -> list[dict]:
    clean_data = []
    dropped_rows = 0

    for row in data:
        clean_row = {}

        # ID
        id_val = str(row.get("id") or "").strip()
        if not id_val:
            logging.warning("Missing id")
            dropped_rows += 1
            continue
        try:
            clean_row["id"] = int(id_val)
        except ValueError:
            logging.warning(f"Invalid id: {id_val}")
            dropped_rows += 1
            continue

        # NAME
        name = str(row.get("name") or "").strip()
        clean_row["name"] = name if name else None

        # AGE
        age_val = str(row.get("age") or "").strip()
        try:
            clean_row["age"] = int(age_val)
        except ValueError:
            logging.warning(f"Invalid age for ID {id_val}: {age_val}")
            clean_row["age"] = None

        # JOIN DATE
        date_val = str(row.get("join_date") or "").strip()
        try:
            dt = datetime.strptime(date_val, "%Y-%m-%d")
            clean_row["join_date"] = dt.strftime("%Y-%m-%d")
        except ValueError:
            logging.warning(f"Invalid join_date for ID {id_val}: {date_val}")
            clean_row["join_date"] = None

        # SALARY
        salary_val = str(row.get("salary") or "").strip()
        try:
            clean_row["salary"] = float(salary_val)
        except ValueError:
            logging.warning(f"Invalif salary for ID {id_val}: {salary_val}")
            clean_row["salary"] = None

        # DEPT
        dept_val = str(row.get("dept") or "").strip()
        clean_row["dept"] = dept_val if dept_val else None

        clean_data.append(clean_row)

    logging.info(f"Rows are cleaning: {len(clean_data)}")
    logging.info(f"Dropped rows: {dropped_rows}")

    return clean_data


# --- STEP 5: SAVE ---
def save_csv(data: list[dict], fp) -> None:
    if not data:
        raise ValueError("Empty dataset")
    with open(fp, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"Ouput file written to {fp}")


# --- PIPELINE ---
def run_pipeline() -> None:
    raw_data = load_csv(fp=INPUT_PATH)
    normalized_data, columns = normalize_columns(raw_data)
    validate_columns(columns)
    clean_data = transform_data(normalized_data)
    save_csv(clean_data, fp=OUTPUT_PATH)


# --- ENTRY POINT ---
if __name__ == "__main__":
    try:
        run_pipeline()
        print("SUCCESS")
    except Exception:
        logging.exception("Pipeline failed")
        print("FAILED")
