# Data Pipeline - Trip Analytics Platform

## 1) Overview

- `src/ingest.py`: streams `output.csv` into PostgreSQL table `trips`.
- `sql/queries.sql`: required SQL analyses.
- `src/api.py`: REST API backed by PostgreSQL.

## 2) Prerequisites

- Python 3.12+
- Docker + Docker Desktop Kubernetes
- kubectl
- psql

External PostgreSQL assumption:

- PostgreSQL is external to the application (managed DB or Postgres running on your machine).
- Configuration is via `DATABASE_URL` only.

## 3) Local Run (No Docker)

1) Create venv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

2) Set `DATABASE_URL`:

```bash
export DATABASE_URL='postgresql://<DB_USER>:<DB_PASSWORD>@<DB_HOST>:5432/<DB_NAME>'
```

3) Run ingestion:

```bash
python src/ingest.py --csv output.csv --db-url "$DATABASE_URL"
```

4) Run API:

```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000
```

5) Curl examples:

```bash
curl -i http://localhost:8000/health
```

Get real IDs from the database:

```bash
DRIVER_ID=$(psql "$DATABASE_URL" -Atc "SELECT driver_id FROM trips LIMIT 1;")
CLIENT_ID=$(psql "$DATABASE_URL" -Atc "SELECT client_id FROM trips LIMIT 1;")
echo "$DRIVER_ID"
echo "$CLIENT_ID"
```

```bash
curl -i "http://localhost:8000/drivers/${DRIVER_ID}/stats"
curl -i "http://localhost:8000/clients/${CLIENT_ID}/trips"
```

## 4) Running SQL Queries

Connect:

```bash
psql "$DATABASE_URL"
```

Run the provided queries file:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -P pager=off -f sql/queries.sql
```

Example driver stats query:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT driver_id AS \"DRIVER\", COUNT(DISTINCT DATE(trip_date)) AS \"TOTAL_DAYS\", ROUND(100.0 * SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) / COUNT(*))::INT AS \"SUCCESS_RATE\" FROM trips WHERE driver_id = '<driver_id>' GROUP BY driver_id;"
```

Example client trips query (newest first):

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT trip_id, driver_id, trip_date, status FROM trips WHERE client_id = '<client_id>' ORDER BY trip_date DESC;"
```

## 5) Docker Build & Run

Build image:

```bash
docker build -t data-pipeline:latest .
```

Start a local Postgres (external to the app) using Docker:

```bash
docker network create trip-net >/dev/null 2>&1 || true

docker rm -f trip-postgres >/dev/null 2>&1 || true
docker run -d --name trip-postgres --network trip-net \
  -e POSTGRES_USER=tripdata \
  -e POSTGRES_PASSWORD=tripdata \
  -e POSTGRES_DB=tripdata \
  -p 5432:5432 \
  postgres:16-alpine

docker exec trip-postgres sh -c 'for i in $(seq 1 30); do pg_isready -U tripdata -d tripdata >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1'
```

Ingest `output.csv` using the container (note: `output.csv` is not copied into the image; mount it):

```bash
docker run --rm --network trip-net \
  -e DATABASE_URL='postgresql://tripdata:tripdata@trip-postgres:5432/tripdata' \
  -v "$(pwd)/output.csv:/data/output.csv:ro" \
  data-pipeline:latest python src/ingest.py --csv /data/output.csv --db-url 'postgresql://tripdata:tripdata@trip-postgres:5432/tripdata'
```

Run the API container:

```bash
docker rm -f trip-api >/dev/null 2>&1 || true
docker run -d --name trip-api --network trip-net \
  -e DATABASE_URL='postgresql://tripdata:tripdata@trip-postgres:5432/tripdata' \
  -p 8000:8000 \
  data-pipeline:latest
```

Env vars:

- `DATABASE_URL` (required): `postgresql://<user>:<pass>@<host>:<port>/<db>`

## 6) Kubernetes (Docker Desktop)

This deploys only the API. PostgreSQL must be reachable from the Kubernetes cluster.

1) Check kubectl context and cluster reachability:

```bash
kubectl config current-context
kubectl cluster-info
```

If no cluster is reachable, `kubectl` may attempt `http://localhost:8080` and fail.

2) Build the image locally:

```bash
docker build -t data-pipeline:latest .
```

3) Apply manifests:

```bash
kubectl apply -f k8s/
```

4) Create/update the Secret with an external DB URL:

```bash
kubectl create secret generic trip-api-db \
  --from-literal=DATABASE_URL='postgresql://<DB_USER>:<DB_PASSWORD>@<DB_HOST>:5432/<DB_NAME>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

If your Postgres runs on your machine and you are using Docker Desktop Kubernetes, it is typically reachable via `host.docker.internal`:

```bash
kubectl create secret generic trip-api-db \
  --from-literal=DATABASE_URL='postgresql://<DB_USER>:<DB_PASSWORD>@host.docker.internal:5432/<DB_NAME>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

5) Restart and wait for readiness:

```bash
kubectl rollout restart deploy/trip-api
kubectl rollout status deploy/trip-api --timeout=60s
kubectl get pods -l app=trip-api -w
```

6) Port-forward and test:

```bash
kubectl port-forward svc/trip-api 8000:80
curl -i http://localhost:8000/health
```

DB down /health 503 test:

```bash
curl -i http://localhost:8000/health
```

Expected when DB is unreachable:

```text
HTTP/1.1 503 Service Unavailable
{"status":"db_unreachable"}
```

## 7) Smoke Tests

```bash
curl -i http://localhost:8000/health
```

```bash
DRIVER_ID=$(psql "$DATABASE_URL" -Atc "SELECT driver_id FROM trips LIMIT 1;")
CLIENT_ID=$(psql "$DATABASE_URL" -Atc "SELECT client_id FROM trips LIMIT 1;")

curl -i "http://localhost:8000/drivers/${DRIVER_ID}/stats"
curl -i "http://localhost:8000/clients/${CLIENT_ID}/trips" | head -c 800; echo
```

404 cases:

```bash
curl -i http://localhost:8000/drivers/nonexistent/stats
curl -i http://localhost:8000/clients/nonexistent/trips
```

## 8) Troubleshooting

Pods stuck NotReady:

```bash
kubectl get pods -l app=trip-api
kubectl describe pod -l app=trip-api
kubectl logs -l app=trip-api --tail=200
```

DB unreachable:

```bash
psql "$DATABASE_URL" -c "SELECT 1;"
curl -i http://localhost:8000/health
```

Image not found (Kubernetes):

```bash
kubectl describe pod -l app=trip-api
```

Wrong kubectl context:

```bash
kubectl config get-contexts
kubectl config current-context
kubectl cluster-info
```
