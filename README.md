## Data Sync Workflow

This project demonstrates a simple data sync workflow that:
- **Seeds a PostgreSQL config table** with API and HS code settings.
- **Serves mock paginated trade data** via a FastAPI mock API.
- **Fetches data in batches from the API** and **writes it to CSV**, updating the `to_date` in the config.

### 1. Prerequisites

- **Python 3.10+**
- **PostgreSQL** running and accessible at:
  - host: `localhost`
  - port: `5434`
  - database: `config-db`
  - user: `sourin`
  - password: `admin`
- Recommended: create and activate a virtual environment.

Install Python dependencies (adjust as needed):

```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary pandas requests
```

### 2. Database setup

Create the `config` table and insert a sample configuration:

```bash
python db_setup.py
```

This will:
- Create the `config` table (if it does not exist).
- Seed one active row in `config` with base URL, API key, dates, HS code, etc.

### 3. Start the mock API

Run the FastAPI mock server defined in `mock_api.py`:

```bash
uvicorn mock_api:app --reload --host 127.0.0.1 --port 8000
```

The process script expects the base URL to be `http://127.0.0.1:8000`.

### 4. Run the processing script (DB-driven)

By default, `process.py`:
- Connects to PostgreSQL.
- Reads all active rows from the `config` table.
- For each config, builds the API URL and fetches data in ranges of **10** records.
- Writes each batch directly to a CSV in the `output/` directory.
- Updates the row’s `to_date` to the last date found in the data.

Run:

```bash
python process.py
```

Output files will be created in `output/` with filenames based on `hs_code`, for example:

- `output/HS_Code-85.csv`

### 5. Run the processing script with a direct URL

You can bypass the database and call the API directly using the CLI options:

```bash
python process.py \
  --url "http://127.0.0.1:8000/API123/India/export/2025-06-01/2025-06-30/0-10/and/HS_Code-85" \
  --output "output/HS_Code-85.csv" \
  --step 10 \
  --config-to-date "2025-06-01"
```

- **`--url`**: full API URL including the initial `0-10` range segment.
- **`--output`**: path to the CSV file to write to.
- **`--step`**: range size for pagination (default `10`).
- **`--config-to-date`**: date string \(YYYY-MM-DD\); if this date is **today or later**, the script skips processing.

### 6. Workflow summary

- **Step 1**: Ensure PostgreSQL is running with the expected DB and credentials.
- **Step 2**: Run `db_setup.py` once to create and seed the `config` table.
- **Step 3**: Start the FastAPI mock server with `uvicorn mock_api:app`.
- **Step 4**: Run `python process.py` to fetch paginated data and write CSV files under `output/`.

