# python-etl-scripts

Python ETL scripts (Data Pipeline Foundations): CSV processing, API ingestion, and PostgreSQL loading with logging and error handling

## Overview

This repository contains three mini data pipelines built using Python and PostgreSQL.
The goal is to demonstrate core data engineering concepts: ingestion, validation, transformation, loading, and logging.

Each pipeline is designed with production-oriented thinking:
* Explicit input/output handling
* Schema validation
* Error handling
* Logging
* Re-runnability (idempotent behavior where applicable)

---

## Pipelines

### 1. CSV Cleaning Pipeline (csv_cleaner.py and csv_cleaner_pandas.py)

**Flow:** CSV → Clean → Processed CSV

**What it does:**
* Reads raw CSV data
* Normalizes and Validates required columns
* Cleans and standardizes data (null handling, formatting, type conversion)
* Writes cleaned data

---

### 2. API to CSV Pipeline (weather_api_pipeline.py)

**Flow:** API → JSON → Transform → CSV

**What it does:**
* Fetches data from a public API
* Parses JSON response
* Flattens and transforms fields
* Saves structured output as CSV

---

### 3. CSV to PostgreSQL Loader (csv_to_postgres.py)

**Flow:** CSV → PostgreSQL

**What it does:**
* Reads cleaned CSV data
* Validates schema
* Creates table (if not exists)
* Inserts data into PostgreSQL

---

## Edge Cases Handled
- Missing input file
- Invalid schema
- Empty dataset

---

## Project Structure

```
(root) python-etl-scripts/
│
├── src/
│   ├── __init__.py
│   ├── csv_cleaner.py
│   ├── csv_cleaner_pandas.py
│   ├── csv_to_postgres.py
│   └── weather_api_pipeline.py
│
├── tests/
│   ├── data/
│   ├── expected/
│   ├── test_csv_cleaner.py
│   └── test_csv_cleaner_pandas.py
│
├── .env
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone the repository

```
git clone https://github.com/rajaajaykumar/python-etl-scripts
cd python-etl-scripts
```

### 2. Install dependencies

```
pip install -r requirements.txt
```

### 3. Setup PostgreSQL

* Install PostgreSQL locally
* Create a database
* Update connection details in config

---

## How to Run

### Run CSV Cleaning Pipeline

```
python scripts/csv_cleaner.py
```

### Run API Pipeline

```
python scripts/weather_api_pipeline.py
```

### Run PostgreSQL Loader

```
python scripts/csv_to_postgres.py
```

---

## Logging

* Logs are written to:

  ```
  /logs/pipeline.log
  ```

* Includes:
  * Execution start/end
  * Row counts
  * Errors and failures

---

## Design Decisions

* **Pandas** used for data transformation (faster and industry standard)
* **Logging** implemented for traceability and debugging
* **Modular functions** (extract, transform, load pattern)
* **Basic validation** added to handle bad data inputs
* **Config-driven paths** (avoids hardcoding)

---

## Assumptions

* Input CSV files follow a semi-structured format
* API responses are consistent (basic validation added)
* PostgreSQL is available locally

