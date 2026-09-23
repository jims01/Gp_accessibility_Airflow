# GP Accessibility Pipeline

An automated, orchestrated data pipeline that measures GP (general practitioner) surgery
accessibility across UK wards built to demonstrate core data engineering practices:
ETL design, dimensional modeling with slowly changing dimensions, spatial analysis, and
workflow orchestration with Apache Airflow.

## What it does

For a given UK city, the pipeline:

1. Extracts ward boundary polygons from ONS (Office for National Statistics) open data
2. Extracts GP surgery locations from OpenStreetMap
3. Maintains a versioned history of ward boundaries (Slowly Changing Dimension, Type 2) -
   so changes to ward definitions over time are tracked, not overwritten
4. Counts how many GP surgeries fall within 1km of each ward's centroid
5. Loads the results into a star-schema fact table for analysis

The whole process runs as a single Airflow DAG, scheduled to run daily, with each step
implemented as an independent, retryable task.

## Why

Access to healthcare varies significantly by area. This pipeline provides a repeatable,
automated way to measure one dimension of that GP density per ward as a foundation
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

- **`staging_wards`** / **`staging_gp_locations`** - raw landing tables for each pipeline run,
  loosely typed, no constraints (staging data doesn't need the rigor of the final schema)
- **`dim_wards`** - a Type 2 slowly changing dimension: every version of a ward's data is kept,
  with `valid_from` / `valid_to` / `is_current` tracking which version is live at any point in time
- **`fact_gp_count`** - one row per ward per measurement, referencing `dim_wards` by its
  surrogate key (`ward_id`), so historical counts stay correctly linked to the ward version
  that was current when they were measured

## Tech stack

- **Python** - pandas, GeoPandas, SQLAlchemy, OSMnx
- **Apache Airflow 3** (TaskFlow API) - orchestration, running in WSL2/Ubuntu
- **PostgreSQL + PostGIS** (hosted on Supabase) - storage, spatial queries
- **Data sources** - ONS ward/local authority boundaries (GeoJSON), OpenStreetMap via OSMnx

## Running it locally

1. Set up WSL2 + Ubuntu, and a Python virtual environment
2. `pip install apache-airflow==3.3.0 --constraint <official constraints file>`
3. `pip install geopandas osmnx psycopg2-binary python-dotenv geoalchemy2`
4. Create a `.env` file with `DB_PASSWORD` for your Postgres connection
5. Enable the PostGIS extension on your database: `CREATE EXTENSION IF NOT EXISTS postgis;`
6. Place `gp_accessibility_pipeline.py` in your `$AIRFLOW_HOME/dags/` folder
7. `airflow standalone`, then trigger `gp_accessibility_pipeline` from the UI at `localhost:8080`

## Known limitations

- Currently hardcoded to a single city (Newcastle upon Tyne) - not yet parameterized
- `extract_wards` matches places by exact name against a single Local Authority District,
  so it doesn't support multi-borough regions like Greater London
- Credentials are read via `.env`, not Airflow's built-in Connections - a reasonable
  simplification for a learning project, but not how a production system would handle secrets
- dbt transformations and tests are run manually, as a separate step, rather than orchestrated
  by this DAG - a deliberate choice for now, documented rather than automated

## Challenges worked through

A few of the real problems debugged while building this (rather than a clean, idealized
build — genuinely useful to know if extending this):

- **CRS mismatches** - ward boundaries and GP points arrive in different coordinate systems
  (`EPSG:4326` vs `EPSG:27700`); spatial joins fail silently (zero matches, no error) if this
  isn't handled before any distance-based analysis
- **GeoPandas' "active geometry" tracking** - renaming a geometry column with a plain
  `.rename()` changes its label but not GeoPandas' internal pointer to which column is
  the actual geometry; `.rename_geometry()` is required instead
- **XCom's serialization limits** - Airflow's default task-to-task messaging can't handle
  a `pandas.DataFrame` (or even a `pandas.Timestamp` inside one) directly; data has to be
  converted to plain types (e.g. `.to_dict(orient="records")`) before being returned
- **PostGIS geometry type strictness** - UK ward boundaries include `MultiPolygon` shapes
  (wards split into multiple pieces), which a column typed strictly as `Polygon` will reject

## Possible next steps

- Parameterize the pipeline to run for multiple cities
- Add a dbt layer with tests (`not_null`, `unique`, `relationships`) on top of `fact_gp_count`
- Move credentials to Airflow Connections
- Containerize with Docker

---

Built as part of a self-directed data engineering learning project.# Gp_accessibility_Airflow
