import argparse
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

# Docker container: postgres-db-3 on port 5434
DATABASE_URL = "postgresql+psycopg2://sourin:admin@localhost:5434/config-db"

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

RANGE_STEP = 10
OUTPUT_DIR = "output"


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
            print("No active configurations found.")
            return

        with create_http_session() as http:
            for config in configs:
                print(f"\nProcessing HS Code: {config.hs_code}")

                config_to_date = (
                    config.to_date.date()
                    if isinstance(config.to_date, datetime)
                    else config.to_date
                )

                today = date.today()

                print(f"  Config to_date: {config_to_date}")
                print(f"  Today         : {today}")

                if config_to_date >= today:
                    print(
                        f"  Skipping — config to_date ({config_to_date}) is today or in the future. "
                        f"No new data expected."
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
                    print(f"url: {url}  | range: {range_start}-{range_end}")

                    try:
                        response = http.get(url, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                        print(f"  Received {len(data)} records")
                    except Exception as e:
                        print(f"  Request failed for {url}: {e}")
                        break

                    if not data:
                        break

                    # Write this batch to CSV immediately
                    write_batch_to_csv(file_path, data, write_header=first_batch)
                    first_batch = False
                    total_records += len(data)

                    print(f"  Batch {range_start}-{range_end}: wrote {len(data)} records")

                    # -------------------------------------------------------
                    # CHANGED: Track last date and update DB after every batch
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
                                    # CHANGED: Update config and commit after every batch
                                    config.to_date = batch_date
                                    session.commit()
                                    print(f"  config date: {config.to_date} Updated config {config.id} to_date to {batch_date} (batch {range_start}-{range_end})")
                        except Exception as e:
                            print(f"  Date parsing failed for batch {range_start}-{range_end}: {e}")
                    # -------------------------------------------------------
                    # END OF CHANGE
                    # -------------------------------------------------------

                    range_start += RANGE_STEP

                if total_records == 0:
                    print("  No data found.")
                    continue

                print(f"  Saved {total_records} total records to {file_path}")

                # CHANGED: Removed the post-loop `if last_date:` update block — now handled per batch above

    finally:
        session.close()


# -------------------------------------------------------
# Direct URL processing (CLI mode)
# -------------------------------------------------------
def fetch_all_from_url(url, range_step=RANGE_STEP, output_file=None, config_to_date=None):
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    segments = parsed.path.lstrip("/").split("/")

    if len(segments) < 7:
        print("URL path does not match expected API format.")
        return

    if config_to_date is not None:
        if isinstance(config_to_date, str):
            try:
                config_to_date = datetime.strptime(config_to_date, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid config_to_date format '{config_to_date}'. Expected YYYY-MM-DD.")
                return

        today = date.today()

        print(f"Config to_date: {config_to_date}")
        print(f"Today         : {today}")

        if config_to_date >= today:
            print(
                f"Skipping — config to_date ({config_to_date}) is today or in the future. "
                f"No new data expected."
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
                print(f"Request failed for {full_url}: {e}")
                break

            if not data:
                break

            write_batch_to_csv(output_file, data, write_header=first_batch)
            first_batch = False
            total_records += len(data)

            print(f"Batch {range_start}-{range_end}: wrote {len(data)} records")

            range_start += range_step

    if total_records == 0:
        print("No data found at the provided URL.")
        return

    print(f"Saved {total_records} total records to {output_file}")


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