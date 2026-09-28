# GP Accessibility Pipeline

An automated, orchestrated data pipeline that measures GP (general practitioner) surgery
accessibility across UK wards, built to demonstrate core data engineering practices:
ETL design, dimensional modeling with slowly changing dimensions, spatial analysis,
workflow orchestration with Apache Airflow, containerization with Docker, cloud object
storage on AWS S3, and CI/CD with GitHub Actions.

## What it does

For a given UK city, the pipeline:

1. Reads ward and local-authority boundary polygons (ONS open data) from a private AWS S3 bucket
2. Extracts GP surgery locations from OpenStreetMap
3. Maintains a versioned history of ward boundaries (Slowly Changing Dimension, Type 2), so
   changes to ward definitions over time are tracked, not overwritten
4. Counts how many GP surgeries fall within 1km of each ward's centroid
5. Loads the results into a star-schema fact table for analysis

The whole process runs as a single Airflow DAG, scheduled daily, with each step implemented
as an independent, retryable task.

## Architecture

```
S3 (boundary files) ──▶ extract_wards ─────────────┐
                                                    ├──▶ sync_wards (SCD Type 2) ─┐
                                                    │                              │
OpenStreetMap ──────▶ extract_gp_locations ─────────────────────────────────────────▶ count_gps_per_ward
                                                                                      │
                                                                                      ▼
                                                                                load_fact_gp_counts
```

Ward and GP data are staged in Postgres (not passed directly between tasks) because
Airflow's task-to-task messaging (XCom) is designed for small values, not full spatial
datasets. Only the final, small, aggregated count table is passed directly between tasks.

## Data model

- **`staging_wards`** / **`staging_gp_locations`**: landing tables, emptied and reloaded on every
  run so that re-running the pipeline never duplicates data
- **`dim_wards`**: a Type 2 slowly changing dimension. Every version of a ward's data is kept,
  with `valid_from` / `valid_to` / `is_current` tracking which version is live at any point in time
- **`fact_gp_count`**: one row per ward per measurement, referencing `dim_wards` by its
  surrogate key (`ward_id`), so historical counts stay linked to the ward version that was
  current when they were measured. This table is append-only by design: it is the history

## Tech stack

- **Python**: pandas, GeoPandas, SQLAlchemy, OSMnx, boto3
- **Apache Airflow 3** (TaskFlow API): orchestration
- **Docker + Docker Compose**: containerized deployment
- **AWS S3 + IAM**: private object storage for boundary data, read via a least-privilege user
- **PostgreSQL + PostGIS** (hosted on Supabase): storage and spatial queries
- **GitHub Actions + GHCR**: CI checks and image publishing
- **Data sources**: ONS ward and local authority boundaries (GeoJSON), OpenStreetMap via OSMnx

## Running it with Docker

### Prerequisites

1. [Docker Desktop](https://docs.docker.com/desktop/install/windows-install/) (WSL 2 backend on Windows), running
2. A Postgres database with PostGIS enabled (`CREATE EXTENSION IF NOT EXISTS postgis;`) and the
   four tables described above
3. An AWS account with a **private** S3 bucket containing the two ONS boundary files under a
   `spatial/` prefix (copies are in `spatial_data/` in this repo, ready to upload)
4. An IAM user for the pipeline with read-only access scoped to that prefix:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject"],
         "Resource": "arn:aws:s3:::<your-bucket-name>/spatial/*"
       }
     ]
   }
   ```

### Run

1. Set `S3_BUCKET` near the top of `dags/gp_accessibility_pipeline.py` to your bucket name
2. Export credentials in your terminal. They are read from the environment and are never
   written to a file or committed:
   ```bash
    export DB_PASSWORD='...'
    export AWS_ACCESS_KEY_ID='...'
    export AWS_SECRET_ACCESS_KEY='...'
    export AWS_DEFAULT_REGION='eu-west-2'
   ```
3. From the repo root:
   ```bash
   docker compose up --build
   ```
4. Open `localhost:8080`. The admin password is printed in the startup logs, or retrievable with:
   ```bash
   docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```
5. Trigger `gp_accessibility_pipeline` from the UI

Changes to files in `dags/` are picked up live (mounted as a volume). Rebuild only when
`requirements.txt` or the `Dockerfile` changes.

## CI/CD

The workflow in `.github/workflows/docker-build.yml` runs on every push:

1. **Build**: builds the Docker image on a fresh GitHub-hosted machine
2. **Smoke test**: runs `airflow db check` inside the image to confirm Airflow initializes
3. **Publish** (only if the build job passed, and only from `main`): pushes the image to the
   GitHub Container Registry

```bash
docker pull ghcr.io/jims01/gp_accessibility_airflow:latest
```

The image provides Airflow and every dependency. The DAG code itself is mounted from
`dags/` at runtime, so running the pipeline still needs this repo alongside the image.
Anonymous pulls only work once the package's visibility is set to public.

The smoke test proves the image builds and Airflow starts inside it. It does not run the
pipeline or connect to the real database, and it isn't meant to.

## Known limitations

- Hardcoded to a single city (Newcastle upon Tyne)
- `extract_wards` matches places by exact name against a single Local Authority District, so it
  does not support multi-borough regions such as Greater London
- The S3 bucket name is a constant in the DAG file rather than configuration
- Credentials come from environment variables. On AWS itself, an IAM role would be preferable
  to long-lived access keys
- The `spatial_data/` volume mount in `docker-compose.yaml` is no longer used by the DAG and
  can be removed
- dbt transformations and tests are run manually, as a separate step, rather than orchestrated
  by this DAG

## Challenges worked through

Real problems debugged while building this:

- **Duplicated rows on every run.** Staging loads originally used `append`, so each run added
  another full copy. The counting step joins the two staging tables, so with 11 copies of each,
  every GP was counted 11 x 11 = 121 times, silently inflating the results. The fix was to make
  the load idempotent: `TRUNCATE` the staging table after extraction succeeds and just before
  the load. `if_exists="replace"` was rejected because it drops the table and discards the
  hand-designed column types
- **S3 returns 403, not 404, for a missing object** when the caller isn't allowed to list the
  bucket, so a typo in an object key looks like a permissions problem
- **CRS mismatches.** Ward boundaries and GP points arrive in different coordinate systems
  (`EPSG:27700` vs `EPSG:4326`), and spatial joins fail silently (zero matches, no error) if
  this isn't handled first
- **GeoPandas' "active geometry" tracking.** A plain `.rename()` changes a geometry column's
  label but not GeoPandas' internal pointer to it; `.rename_geometry()` is required
- **XCom serialization limits.** Airflow's default task-to-task messaging can't carry a
  `pandas.DataFrame` or a `pandas.Timestamp`; data has to be converted to plain types first
- **PostGIS type strictness.** Ward boundaries include `MultiPolygon` shapes, which a column
  typed strictly as `Polygon` rejects
- **Container isolation.** A container can't see the host's files or shell variables by default;
  volumes and passed-through environment variables bridge exactly what is needed

## Possible next steps

- Parameterize the pipeline to run for multiple cities
- Add a dbt layer with tests (`not_null`, `unique`, `relationships`) on top of `fact_gp_count`
- Provide the same storage layer on Azure Blob Storage and compare it with S3
- Move secrets to a proper secrets manager or Airflow Connections

---

Built as part of a self-directed data engineering learning project.
