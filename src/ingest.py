import argparse
import csv
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

import psycopg


EXPECTED_HEADER = ["trip_id", "client_id", "driver_id", "trip_date", "status"]
ALLOWED_STATUS = {"done", "not_respond"}

DEFAULT_CSV_PATH = "output.csv"
BATCH_SIZE = 1000
LOG_EVERY_N_ROWS = 50_000


@dataclass
class Counters:
    total_rows_read: int = 0
    inserted: int = 0
    duplicates_skipped: int = 0
    invalid_date: int = 0
    invalid_status: int = 0
    other_errors: int = 0

    @property
    def valid_rows(self) -> int:
        return (
            self.total_rows_read
            - self.invalid_date
            - self.invalid_status
            - self.other_errors
        )


def parse_trip_date(date_str: str) -> Optional[datetime]:
    s = date_str.strip()
    if not s:
        return None

    try:
        return datetime.fromisoformat(s.replace(" ", "T"))
    except ValueError:
        pass

    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        pass

    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def ensure_schema(conn: psycopg.Connection) -> None:
    logging.info("Ensuring schema exists")
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
              trip_id TEXT PRIMARY KEY,
              client_id TEXT NOT NULL,
              driver_id TEXT NOT NULL,
              trip_date TIMESTAMP NOT NULL,
              status TEXT NOT NULL CHECK (status IN ('done','not_respond'))
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_trips_driver_date ON trips(driver_id, trip_date);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_trips_client_date ON trips(client_id, trip_date);"
        )
    conn.commit()


def _insert_batch(
    conn: psycopg.Connection,
    rows: list[tuple[str, str, str, datetime, str]],
) -> tuple[int, int]:
    if not rows:
        return 0, 0

    sql = (
        "INSERT INTO trips (trip_id, client_id, driver_id, trip_date, status) "
        "VALUES (%s, %s, %s, %s, %s) "
        "ON CONFLICT (trip_id) DO NOTHING"
    )

    with conn.cursor() as cur:
        try:
            cur.executemany(sql, rows)
            inserted = cur.rowcount
            duplicates = len(rows) - inserted
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return inserted, duplicates


def _row_to_insert_tuple(row: dict[str, str], counters: Counters) -> Optional[tuple[str, str, str, datetime, str]]:
    try:
        trip_id = (row.get("trip_id") or "").strip()
        client_id = (row.get("client_id") or "").strip()
        driver_id = (row.get("driver_id") or "").strip()
        trip_date_raw = (row.get("trip_date") or "").strip()
        status = (row.get("status") or "").strip()

        if not trip_id or not client_id or not driver_id or not trip_date_raw or not status:
            counters.other_errors += 1
            return None

        if status not in ALLOWED_STATUS:
            counters.invalid_status += 1
            return None

        trip_dt = parse_trip_date(trip_date_raw)
        if trip_dt is None:
            counters.invalid_date += 1
            return None

        return (trip_id, client_id, driver_id, trip_dt, status)
    except Exception:
        counters.other_errors += 1
        return None


def ingest_csv(conn: psycopg.Connection, csv_path: str) -> Counters:
    counters = Counters()
    batch: list[tuple[str, str, str, datetime, str]] = []

    # utf-8-sig is BOM-tolerant while still reading utf-8 content.
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        if reader.fieldnames != EXPECTED_HEADER:
            raise ValueError(
                f"CSV header mismatch. Expected {EXPECTED_HEADER} but got {reader.fieldnames}"
            )

        for row in reader:
            counters.total_rows_read += 1

            insert_row = _row_to_insert_tuple(row, counters)
            if insert_row is not None:
                batch.append(insert_row)

            if len(batch) >= BATCH_SIZE:
                inserted, duplicates = _insert_batch(conn, batch)
                counters.inserted += inserted
                counters.duplicates_skipped += duplicates
                batch.clear()

            if (
                counters.total_rows_read % LOG_EVERY_N_ROWS == 0
                and counters.total_rows_read > 0
            ):
                logging.info(
                    "Progress: read=%d inserted=%d dup=%d invalid_date=%d invalid_status=%d other=%d",
                    counters.total_rows_read,
                    counters.inserted,
                    counters.duplicates_skipped,
                    counters.invalid_date,
                    counters.invalid_status,
                    counters.other_errors,
                )

    if batch:
        inserted, duplicates = _insert_batch(conn, batch)
        counters.inserted += inserted
        counters.duplicates_skipped += duplicates

    return counters


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ingest output.csv into PostgreSQL")
    p.add_argument(
        "--csv",
        default=DEFAULT_CSV_PATH,
        help=f"Path to CSV file (default: {DEFAULT_CSV_PATH})",
    )
    p.add_argument(
        "--db-url",
        default=None,
        help="PostgreSQL connection URL (or set DATABASE_URL env var)",
    )
    return p


def _resolve_db_url(arg_db_url: Optional[str]) -> str:
    db_url = (arg_db_url or "").strip() or os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise ValueError("Missing --db-url (or set DATABASE_URL env var)")
    return db_url


def _print_report(counters: Counters, duration_seconds: float) -> None:
    throughput = (
        (counters.total_rows_read / duration_seconds) if duration_seconds > 0 else 0.0
    )

    print("Ingestion completed successfully")
    print("")
    print("Summary:")
    print(f"  Total rows read:        {counters.total_rows_read:,}")
    print(f"  Valid rows:             {counters.valid_rows:,}")
    print(f"  Successfully inserted:  {counters.inserted:,}")
    print(f"  Skipped (duplicates):   {counters.duplicates_skipped:,}")
    print("")
    print("Errors:")
    print(f"  Invalid date format:    {counters.invalid_date:,}")
    print(f"  Invalid status value:   {counters.invalid_status:,}")
    print(f"  Other errors:           {counters.other_errors:,}")
    print("")
    print(f"Duration: {duration_seconds:.2f} seconds")
    print(f"Throughput: {throughput:,.2f} rows/sec")


def main(argv: Optional[Iterable[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    args = build_parser().parse_args(list(argv) if argv is not None else None)

    try:
        db_url = _resolve_db_url(args.db_url)
        csv_path = args.csv

        start = time.perf_counter()
        with psycopg.connect(db_url) as conn:
            ensure_schema(conn)
            counters = ingest_csv(conn, csv_path)
        duration = time.perf_counter() - start
        _print_report(counters, duration)
        return 0
    except Exception:
        logging.exception("Ingestion failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
