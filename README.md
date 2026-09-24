# GP Accessibility Pipeline

An automated, orchestrated data pipeline that measures GP (general practitioner) surgery
accessibility across UK wards — built to demonstrate core data engineering practices:
ETL design, dimensional modeling with slowly changing dimensions, spatial analysis,
workflow orchestration with Apache Airflow, and containerization with Docker.

## What it does

For a given UK city, the pipeline:

1. Extracts ward boundary polygons from ONS (Office for National Statistics) open data
2. Extracts GP surgery locations from OpenStreetMap
3. Maintains a versioned history of ward boundaries (Slowly Changing Dimension, Type 2) —
   so changes to ward definitions over time are tracked, not overwritten
4. Counts how many GP surgeries fall within 1km of each ward's centroid
5. Loads the results into a star-schema fact table for analysis

The whole process runs as a single Airflow DAG, scheduled to run daily, with each step
implemented as an independent, retryable task. It can be run directly on a machine with
Airflow installed, or fully containerized with Docker Compose.

## Why

Access to healthcare varies significantly by area. This pipeline provides a repeatable,
automated way to measure one dimension of that — GP density per ward — as a foundation
for further analysis (e.g. comparing accessibility against population or deprivation data).

## Architecture

```
extract_wards ─────────────┐
                            ├──▶ sync_wards (SCD Type 2) ─┐
                            │                              │
extract_gp_locations ───────────────────────────────────────▶ count_gps_per_ward
                                                              │
                                                              ▼
                                                        load_fact_gp_counts
```

Ward and GP data are staged in Postgres (not passed directly between tasks) because
Airflow's task-to-task messaging (XCom) is designed for small values, not full spatial
datasets. Only the final, small, aggregated count table is passed directly between tasks.

## Data model

- **`staging_wards`** / **`staging_gp_locations`** — raw landing tables for each pipeline run,
  loosely typed, no constraints (staging data doesn't need the rigor of the final schema)
- **`dim_wards`** — a Type 2 slowly changing dimension: every version of a ward's data is kept,
  with `valid_from` / `valid_to` / `is_current` tracking which version is live at any point in time
- **`fact_gp_count`** — one row per ward per measurement, referencing `dim_wards` by its
  surrogate key (`ward_id`), so historical counts stay correctly linked to the ward version
  that was current when they were measured

## Tech stack

- **Python** — pandas, GeoPandas, SQLAlchemy, OSMnx
- **Apache Airflow 3** (TaskFlow API) — orchestration
- **Docker + Docker Compose** — containerized, portable deployment
- **PostgreSQL + PostGIS** (hosted on Supabase) — storage, spatial queries
- **Data sources** — ONS ward/local authority boundaries (GeoJSON, included in `spatial_data/`),
  OpenStreetMap via OSMnx

## Running it with Docker (recommended)

The whole pipeline — Airflow, all dependencies, and its data files — runs as a single
container, with no manual Python/package setup required.

1. Install [Docker Desktop](https://docs.docker.com/desktop/install/windows-install/)
   (WSL 2 backend, if on Windows)
2. Enable the PostGIS extension once on your Postgres database: `CREATE EXTENSION IF NOT EXISTS postgis;`
3. Set your database password as an environment variable (never committed to the repo):
   ```bash
   export DB_PASSWORD='your_actual_password'
   ```
4. From the repo root:
   ```bash
   docker compose up --build
   ```
5. Once it's running, open `localhost:8080` and log in. The admin password is printed in
   the startup logs, or retrievable with:
   ```bash
   docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```
6. Trigger `gp_accessibility_pipeline` from the UI

DAG and data-file changes made on your host machine are picked up live (mounted as
volumes) — no rebuild needed unless `requirements.txt` or the `Dockerfile` itself changes.

## Running it locally (without Docker)

1. Set up WSL2 + Ubuntu, and a Python virtual environment
2. `pip install apache-airflow==3.3.0 --constraint <official constraints file>`
3. `pip install geopandas osmnx psycopg2-binary python-dotenv geoalchemy2`
4. Enable PostGIS: `CREATE EXTENSION IF NOT EXISTS postgis;`
5. Export `DB_PASSWORD` as an environment variable (see above)
6. Place `gp_accessibility_pipeline.py` and `spatial_data/` in your `$AIRFLOW_HOME/dags/` folder
7. `airflow standalone`, then trigger `gp_accessibility_pipeline` from the UI at `localhost:8080`

## Known limitations

- Currently hardcoded to a single city (Newcastle upon Tyne) — not yet parameterized
- `extract_wards` matches places by exact name against a single Local Authority District,
  so it doesn't support multi-borough regions like Greater London
- dbt transformations and tests are run manually, as a separate step, rather than orchestrated
  by this DAG — a deliberate choice for now, documented rather than automated

## Challenges worked through

A few of the real problems debugged while building this (rather than a clean, idealized
build — genuinely useful to know if extending this):

- **CRS mismatches** — ward boundaries and GP points arrive in different coordinate systems
  (`EPSG:4326` vs `EPSG:27700`); spatial joins fail silently (zero matches, no error) if this
  isn't handled before any distance-based analysis
- **GeoPandas' "active geometry" tracking** — renaming a geometry column with a plain
  `.rename()` changes its label but not GeoPandas' internal pointer to which column is
  the actual geometry; `.rename_geometry()` is required instead
- **XCom's serialization limits** — Airflow's default task-to-task messaging can't handle
  a `pandas.DataFrame` (or even a `pandas.Timestamp` inside one) directly; data has to be
  converted to plain types (e.g. `.to_dict(orient="records")`) before being returned
- **PostGIS geometry type strictness** — UK ward boundaries include `MultiPolygon` shapes
  (wards split into multiple pieces), which a column typed strictly as `Polygon` will reject
- **Container filesystem isolation** — a Docker container can't see the host machine's files
  by default; volumes bridge specific folders (DAGs, data files) in, and credentials are
  injected as environment variables rather than read from a `.env` file

## Possible next steps

- Parameterize the pipeline to run for multiple cities
- Add a dbt layer with tests (`not_null`, `unique`, `relationships`) on top of `fact_gp_count`
- Move credentials to Airflow Connections
- Push the image to a container registry (e.g. Docker Hub) for one-command deployment

---

Built as part of a self-directed data engineering learning project.
