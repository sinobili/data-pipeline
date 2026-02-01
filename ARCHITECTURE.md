# ARCHITECTURE.md (Bonus)

This document is a short, non-required overview of the intended architecture for the case submission.

## Components
- `ingest.py` (Python)
  - Reads `output.csv` (semicolon-delimited, quoted)
  - Validates/parses fields
  - Writes into PostgreSQL table `trips` (idempotent inserts)

- PostgreSQL
  - Stores ingested trip records
  - Provides query performance via indexes for driver/client lookups

- API service (FastAPI or Flask)
  - `GET /drivers/{driver_id}/stats`
  - `GET /clients/{client_id}/trips`
  - `GET /health`
  - Reads from PostgreSQL and returns JSON responses

- Docker
  - Multi-stage build for small runtime image
  - Runs as a non-root user
  - Uses environment variables for DB configuration

- Kubernetes
  - Deployment with multiple replicas, resource requests/limits
  - Service for stable access
  - ConfigMap/Secret for database configuration
  - HTTP probes targeting `/health`

## Design Decisions (Rationale)
- External PostgreSQL
  - The case requests Kubernetes manifests for the API service only. PostgreSQL is assumed to be an external dependency (documented in `README.md`).

- Minimal dependencies
  - Prefer streaming CSV ingestion + batched inserts to avoid loading the entire dataset into memory and to keep the Docker image lean.

- Health probes
  - Readiness should reflect whether the service can serve requests (typically DB connectivity).
  - Liveness should avoid restart loops; it may be implemented as a simple process/service check, depending on the chosen approach.

## Data Flow
1. Run ingestion: `output.csv` -> `trips` (PostgreSQL)
2. Serve API: API reads `trips` and returns driver stats / client trip history
3. Deploy: containerize the API; apply Kubernetes manifests
