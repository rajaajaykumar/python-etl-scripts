import csv
import pytest
from src.csv_cleaner import (
    load_csv,
    normalize_columns,
    validate_columns,
    transform_data,
)

SCHEMA = {
    "id": int,
    "age": int,
    "salary": float,
    "name": str,
    "dept": str,
    "join_date": str,
}


# --- READ ---
def read_csv(fp):
    with open(fp, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# --- NORMALIZE ---
def normalize(rows, schema=SCHEMA):
    for row in rows:
        for col, dtype in schema.items():
            val = row.get(col)
            if val is None or val == "":
                row[col] = None
            else:
                try:
                    row[col] = dtype(val)
                except (ValueError, TypeError):
                    row[col] = None
    return rows


# --- MAIN ---
def main(fp):
    data = load_csv(fp)
    norm, cols = normalize_columns(data)
    validate_columns(cols)
    clean = transform_data(norm)
    return clean


# --- TEST 1: Valid ---
def test_valid():
    output = main("tests/data/emp_valid.csv")
    expected = normalize(read_csv("tests/expected/emp_valid_expected.csv"))

    assert output == expected


# --- TEST 2: Invalid ---
def test_invalid():
    output = main("tests/data/emp_invalid.csv")
    expected = normalize(read_csv("tests/expected/emp_invalid_expected.csv"))

    assert output == expected


# --- TEST 3: Missing Id ---
def test_missing_id():
    output = main("tests/data/emp_missing_id.csv")
    expected = read_csv("tests/expected/emp_missing_id_expected.csv")

    assert (len(output)) == 0
    # assert output == expected


# --- TEST 4: Bad Schema ---
def test_bad_schema():
    output = main("tests/data/emp_bad_schema.csv")
    expected = normalize(read_csv("tests/expected/emp_bad_schema_expected.csv"))

    assert output == expected


# --- TEST 5: Empty ---
def test_empty():
    with pytest.raises(ValueError, match="empty"):
        main("tests/data/emp_empty.csv")
