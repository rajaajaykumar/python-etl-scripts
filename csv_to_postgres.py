import os
import csv
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# --- CONFIG ---
INPUT_PATH = "employee.csv"

REQUIRED_COLUMNS = ["id", "name", "age", "join_date", "salary", "dept"]

load_dotenv()
DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD"),
    "host": os.getenv("HOST"),
    "port": "5432",
}

for key, v in DB_CONFIG.items():
    if not v:
        raise ValueError(f"Missing or empty DB config value for: '{key}'")

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")


# --- STEP 1: VALIDATE CSV ---
def validate_csv(fp):
    if not os.path.exists(fp):
        raise FileNotFoundError(f"File not found: {fp}")

    with open(fp, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("CSV has no header")

        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        extra = [c for c in reader.fieldnames if c not in REQUIRED_COLUMNS]
        if extra:
            logging.warning(f"Unexpected columns present: {extra}")

        data = []
        dropped = 0

        for row in reader:
            if not row.get("id"):
                dropped += 1
                continue

            data.append(tuple(row[col] for col in REQUIRED_COLUMNS))

    if not data:
        raise ValueError("No valid data to insert")

    logging.info(f"Validated rows: {len(data)}, Dropped rows: {dropped}")
    return data


# --- STEP 2: DB CONNECTION ---
def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    return conn, cur


# --- STEP 3: CHECK TABLE ---
def check_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS employee (
            id TEXT PRIMARY KEY,
            name TEXT,
            age INT,
            join_date DATE,
            salary NUMERIC,
            dept TEXT
        )
    """
    )


# --- STEP 4: INSERT DATA (BATCH) ---
def insert_data(cur, data):
    query = """
        INSERT INTO employee (id, name, age, join_date, salary, dept)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
        RETURNING id;
    """

    execute_values(cur, query, data)
    inserted_rows = cur.fetchall()

    return len(data), len(inserted_rows)


# --- MAIN ---
def main():
    conn = None
    cur = None

    try:
        data = validate_csv(INPUT_PATH)

        conn, cur = connect_db()

        check_table(cur)

        attempted, inserted = insert_data(cur, data)

        conn.commit()

        logging.info(f"Rows attempted: {attempted}")
        logging.info(f"Rows inserted: {inserted}")
        logging.info(f"Rows skipped: {attempted - inserted}")

    except Exception as e:
        logging.exception("Pipeline failed")

        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
