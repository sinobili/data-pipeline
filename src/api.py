import logging
import os
from datetime import datetime
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()


SQL_DRIVER_STATS = """
SELECT
  driver_id,
  COUNT(DISTINCT DATE(trip_date)) AS total_days,
  ROUND(
    100.0 * SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) / COUNT(*)
  )::INT AS success_rate
FROM trips
WHERE driver_id = %s
GROUP BY driver_id;
""".strip()


SQL_CLIENT_TRIPS = """
SELECT
  trip_id,
  driver_id,
  trip_date,
  status
FROM trips
WHERE client_id = %s
ORDER BY trip_date DESC;
""".strip()


SQL_HEALTH = "SELECT 1;"


def _get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url


def _datetime_to_str(dt: datetime) -> str:
    # Spec does not mandate a trip_date string format; prefer dataset-style for safety.
    # Format: YYYY-MM-DD HH:MM:SS.fff (milliseconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@app.get("/drivers/{driver_id}/stats")
def get_driver_stats(driver_id: str) -> dict[str, Any]:
    logger.info("GET /drivers/%s/stats", driver_id)
    try:
        db_url = _get_db_url()
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_DRIVER_STATS, (driver_id,))
                row = cur.fetchone()
    except HTTPException:
        raise
    except Exception:
        logger.exception("DB error while fetching driver stats")
        raise HTTPException(status_code=503, detail="db_unreachable")

    if row is None:
        raise HTTPException(status_code=404, detail="driver_not_found")

    return {
        "driver_id": row[0],
        "total_days": int(row[1]),
        "success_rate": int(row[2]),
    }


@app.get("/clients/{client_id}/trips")
def get_client_trips(client_id: str) -> list[dict[str, Any]]:
    logger.info("GET /clients/%s/trips", client_id)
    try:
        db_url = _get_db_url()
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_CLIENT_TRIPS, (client_id,))
                rows = cur.fetchall()
    except HTTPException:
        raise
    except Exception:
        logger.exception("DB error while fetching client trips")
        raise HTTPException(status_code=503, detail="db_unreachable")

    if not rows:
        raise HTTPException(status_code=404, detail="client_not_found")

    trips: list[dict[str, Any]] = []
    for trip_id, driver_id, trip_date, status in rows:
        trips.append(
            {
                "trip_id": str(trip_id),
                "driver_id": str(driver_id),
                "trip_date": _datetime_to_str(trip_date),
                "status": str(status),
            }
        )
    return trips


@app.get("/health")
def health() -> Any:
    logger.info("GET /health")
    try:
        db_url = _get_db_url()
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_HEALTH)
                cur.fetchone()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "db_unreachable"})
