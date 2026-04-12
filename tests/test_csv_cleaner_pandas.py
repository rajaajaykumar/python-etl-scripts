import pandas as pd
import pytest
from src.csv_cleaner_pandas import (
    load_csv,
    normalize_columns,
    validate_columns,
    transform_data,
)


# --- READ ---
def read_csv(path):
    df = pd.read_csv(path)
    return df


# --- MAIN ---
def main(path):
    df = load_csv(path)
    norm, cols = normalize_columns(df)
    validate_columns(cols)
    clean = transform_data(norm)
    return clean


# --- TEST 1: Valid ---
def test_valid():
    output = main("tests/data/emp_valid.csv")
    expected = read_csv("tests/expected/emp_valid_expected.csv")

    pd.testing.assert_frame_equal(output, expected, check_dtype=False)


# --- TEST 2: Invalid ---
def test_invalid():
    output = main("tests/data/emp_invalid.csv")
    expected = read_csv("tests/expected/emp_invalid_expected.csv")

    pd.testing.assert_frame_equal(output, expected, check_dtype=False)


# --- TEST 3: Missing Id ---
def test_missing_id():
    output = main("tests/data/emp_missing_id.csv")
    expected = read_csv("tests/expected/emp_missing_id_expected.csv")

    # assert (len(output)) == len(expected)
    pd.testing.assert_frame_equal(output, expected, check_dtype=False)


# --- TEST 4: Bad Schema ---
def test_bad_schema():
    output = main("tests/data/emp_bad_schema.csv")
    expected = read_csv("tests/expected/emp_bad_schema_expected.csv")

    pd.testing.assert_frame_equal(output, expected, check_dtype=False)


# --- TEST 5: Empty ---
def test_empty():
    with pytest.raises(ValueError):
        main("tests/data/emp_empty.csv")
