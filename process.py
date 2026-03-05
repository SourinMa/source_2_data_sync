import argparse
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_setup import Config
import os
import csv
from urllib.parse import urlparse
from datetime import datetime, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import uuid
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Docker container: postgres-db-3 on port 5434
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

RANGE_STEP = 10
OUTPUT_DIR = "output"

# -------------------------------------------------------
# Logging configuration
# -------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------
# HTTP session with connection pooling
# -------------------------------------------------------
def create_http_session():
    session = requests.Session()

    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
    )

    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=retries,
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# -------------------------------------------------------
# Build API URL from config
# -------------------------------------------------------
def build_url(config, range_start, range_end):
    return (
        f"{config.base_url}/{config.api_key}/"
        f"{config.country}/{config.data_type}/"
        f"{config.from_date}/{config.to_date}/"
        f"{range_start}-{range_end}/"
        f"{config.operator}/{config.hs_code}"
    )
    # custom URL format for mock API testing:
    # return f"http://127.0.0.1:8000/{config.api_key}/{config.country}/{config.data_type}/{config.to_date}/2025-10-28/{range_start}-{range_end}/{config.operator}/{config.hs_code}"


# -------------------------------------------------------
# Write a single batch (list of dicts) to CSV incrementally
# -------------------------------------------------------
def write_batch_to_csv(file_path, batch, write_header):
    if not batch:
        return

    fieldnames = list(batch[0].keys())

    with open(file_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(batch)


# -------------------------------------------------------
# Main DB-driven processing
# -------------------------------------------------------
def process():
    session = Session()

    try:
        configs = session.query(Config).filter(Config.active == True).all()

        if not configs:
            logger.info("No active configurations found.")
            return

        with create_http_session() as http:
            for config in configs:
                logger.info("Processing HS Code: %s", config.hs_code)

                run_id = str(uuid.uuid4())
                logger.info("Run ID             : %s", run_id)

                config_to_date = (
                    config.to_date.date()
                    if isinstance(config.to_date, datetime)
                    else config.to_date
                )

                today = date.today()

                logger.info("  Config to_date: %s", config_to_date)
                logger.info("  Today         : %s", today)

                if config_to_date >= today:
                    logger.info(
                        "  Skipping — config to_date (%s) is today or in the future. "
                        "No new data expected.",
                        config_to_date,
                    )
                    continue

                os.makedirs(OUTPUT_DIR, exist_ok=True)
                file_path = os.path.join(OUTPUT_DIR, f"{config.hs_code}.csv")
                open(file_path, "w").close()

                range_start = 0
                total_records = 0
                first_batch = True

                while True:
                    range_end = range_start + RANGE_STEP
                    url = build_url(config, range_start, range_end)
                    logger.info("url: %s  | range: %s-%s", url, range_start, range_end)

                    try:
                        response = http.get(url, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                        logger.info("  Received %d records", len(data))
                    except Exception as e:
                        logger.error("  Request failed for %s: %s", url, e)
                        break

                    if not data:
                        break

                    # Write this batch to CSV immediately
                    try:
                        write_batch_to_csv(file_path, data, write_header=first_batch)
                        first_batch = False
                        total_records += len(data)
                    except Exception as e:
                        logger.error("  Failed to write batch %s-%s to CSV: %s", range_start, range_end, e)
                        break

                    logger.info("  Batch %s-%s: wrote %d records", range_start, range_end, len(data))

                    # -------------------------------------------------------
                    # Track last date and update DB after every batch
                    # -------------------------------------------------------
                    batch_df = pd.DataFrame(data)
                    date_col_candidates = [
                        col for col in batch_df.columns
                        if col.lower() in ("date", "shipment_date", "transaction_date")
                    ]

                    if date_col_candidates:
                        date_col = date_col_candidates[0]
                        try:
                            batch_df[date_col] = pd.to_datetime(batch_df[date_col], errors="coerce")
                            if not batch_df[date_col].isna().all():
                                batch_max = batch_df[date_col].max()
                                if pd.notnull(batch_max):
                                    batch_date = batch_max.date()
                                    config.to_date = batch_date
                                    config.number_of_rows = total_records
                                    config.run_id = run_id
                                    config.updated_date = datetime.utcnow()
                                    session.commit()
                                    logger.info(
                                        "  config date: %s — Updated config %s to_date to %s (batch %s-%s)",
                                        config.to_date, config.id, batch_date, range_start, range_end,
                                    )
                        except Exception as e:
                            logger.warning("  Date parsing failed for batch %s-%s: %s", range_start, range_end, e)
                    else:
                        # No date column — still keep run tracking up to date
                        config.number_of_rows = total_records
                        config.run_id = run_id
                        config.updated_date = datetime.utcnow()
                        session.commit()

                    range_start += RANGE_STEP

                if total_records == 0:
                    logger.info("  No data found.")
                    continue

                logger.info("  Saved %d total records to %s", total_records, file_path)

    finally:
        session.close()


# -------------------------------------------------------
# Direct URL processing (CLI mode)
# -------------------------------------------------------
# Run using CLI:
# python process.py --url "http://127.0.0.1:8000/API123/India/export/2025-06-01/2025-06-30/0-10/and/HS_Code-85" --output "output/HS_Code-85.csv" --step 10 --config-to-date "2025-06-30"

def fetch_all_from_url(url, range_step=RANGE_STEP, output_file=None, config_to_date=None):
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    segments = parsed.path.lstrip("/").split("/")

    if len(segments) < 7:
        logger.error("URL path does not match expected API format.")
        return

    if config_to_date is not None:
        if isinstance(config_to_date, str):
            try:
                config_to_date = datetime.strptime(config_to_date, "%Y-%m-%d").date()
            except ValueError:
                logger.error("Invalid config_to_date format '%s'. Expected YYYY-MM-DD.", config_to_date)
                return

        today = date.today()

        logger.info("Config to_date: %s", config_to_date)
        logger.info("Today         : %s", today)

        if config_to_date >= today:
            logger.info(
                "Skipping — config to_date (%s) is today or in the future. "
                "No new data expected.",
                config_to_date,
            )
            return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    range_idx = -3

    if not output_file:
        hs_code_seg = segments[-1]
        output_file = os.path.join(OUTPUT_DIR, f"{hs_code_seg}.csv")

    open(output_file, "w").close()

    range_start = 0
    total_records = 0
    first_batch = True

    with create_http_session() as http:
        while True:
            range_end = range_start + range_step
            segments[range_idx] = f"{range_start}-{range_end}"
            new_path = "/".join(segments)
            full_url = f"{base}/{new_path}"

            try:
                resp = http.get(full_url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error("Request failed for %s: %s", full_url, e)
                break

            if not data:
                break

            write_batch_to_csv(output_file, data, write_header=first_batch)
            first_batch = False
            total_records += len(data)

            logger.info("Batch %s-%s: wrote %d records", range_start, range_end, len(data))

            range_start += range_step

    if total_records == 0:
        logger.warning("No data found at the provided URL.")
        return

    logger.info("Saved %d total records to %s", total_records, output_file)


# -------------------------------------------------------
# Entry Point
# -------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process API data to CSV")

    parser.add_argument("--url", help="Full API URL to fetch (must include range segment like 0-10)")
    parser.add_argument("--output", help="Output CSV file path")
    parser.add_argument("--step", type=int, default=RANGE_STEP, help="Range step size (default=10)")
    parser.add_argument(
        "--config-to-date",
        dest="config_to_date",
        help="Config to_date (YYYY-MM-DD) to compare against date.today()"
    )

    args = parser.parse_args()

    if args.url:
        fetch_all_from_url(
            url=args.url,
            range_step=args.step,
            output_file=args.output,
            config_to_date=args.config_to_date
        )
    else:
        process()