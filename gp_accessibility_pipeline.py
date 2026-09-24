from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import quote_plus

import geopandas as gpd
import osmnx as ox
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text


def get_engine() -> Engine:
    load_dotenv("/mnt/c/Users/ajayi/geojson/.env")
    db_password = os.environ["DB_PASSWORD"]
    safe_password = quote_plus(db_password)
    connection_string = (
        f"postgresql://postgres.rovrbmivgqzhqcoutjbj:{safe_password}"
        f"@aws-0-eu-central-1.pooler.supabase.com:5432/postgres"
    )
    return create_engine(connection_string)


def extract_wards(place: str) -> gpd.GeoDataFrame:
    """Fetch ward boundaries for a place, reprojected to British National Grid."""
    wards = gpd.read_file("/opt/airflow/spatial_data/Wards_December_2023_Boundaries_UK_BGC_-5149544542375210439.geojson")
    la = gpd.read_file("/opt/airflow/spatial_data/Local_Authority_Districts_December_2023_Boundaries_UK_BUC_1184848993647424420.geojson")

    place_boundary = la[la["LAD23NM"] == place]
    if len(place_boundary) == 0:
        raise ValueError(f"No Local Authority District found matching '{place}'")
    place_wards = gpd.sjoin(wards, place_boundary, how="inner", predicate="intersects")
    place_wards = place_wards.drop(columns=["index_left", "index_right"], errors="ignore")
    return place_wards.to_crs(epsg=27700)


def extract_gp_locations(place: str) -> gpd.GeoDataFrame:
    """Pull GP surgery locations for a place."""
    gps = ox.features_from_place(place, tags={"amenity": "doctors"})
    gps = gps[gps.geometry.type == "Point"]
    if len(gps) == 0:
        raise ValueError(f"No GP surgery location found matching '{place}'")
    print(f"Extracted {len(gps)} GP locations for {place}")
    return gps


def get_current_ward(wd23cd: str, engine: Engine) -> pd.DataFrame:
    """Look up the current live version of a ward, if one exists."""
    query = text("SELECT * FROM dim_wards WHERE wd23cd = :wd23cd AND is_current = true")
    return pd.read_sql(query, engine, params={"wd23cd": wd23cd})


def sync_ward(ward_row: pd.Series, engine: Engine) -> None:
    """Insert or version-update one ward's dim_wards row, Type 2 style."""
    current = get_current_ward(ward_row["WD23CD"], engine)

    if len(current) == 0:
        new_row = pd.DataFrame([{
            "wd23cd": ward_row["WD23CD"],
            "ward_name": ward_row["WD23NM"],
            "local_authority": ward_row["LAD23NM"],
            "valid_from": datetime.now().date(),
            "valid_to": None,
            "is_current": True,
        }])
        new_row.to_sql("dim_wards", engine, if_exists="append", index=False)
        print(f"Inserted new ward: {ward_row['WD23NM']}")
    else:
        if (
            ward_row["WD23NM"] != current["ward_name"].iloc[0]
            or ward_row["LAD23NM"] != current["local_authority"].iloc[0]
        ):
            update_query = text("""
                UPDATE dim_wards
                SET valid_to = :today, is_current = false
                WHERE wd23cd = :wd23cd AND is_current = true
            """)
            with engine.connect() as conn:
                conn.execute(update_query, {"today": datetime.now().date(), "wd23cd": ward_row["WD23CD"]})
                conn.commit()

            new_row = pd.DataFrame([{
                "wd23cd": ward_row["WD23CD"],
                "ward_name": ward_row["WD23NM"],
                "local_authority": ward_row["LAD23NM"],
                "valid_from": datetime.now().date(),
                "valid_to": None,
                "is_current": True,
            }])
            new_row.to_sql("dim_wards", engine, if_exists="append", index=False)
            print(f"Ward changed, new version saved: {ward_row['WD23NM']}")
        else:
            print(f"No change: {ward_row['WD23NM']}")


def sync_all_wards(wards: gpd.GeoDataFrame, engine: Engine) -> None:
    for index, row in wards.iterrows():
        sync_ward(row, engine)


def count_gps_per_ward(wards: gpd.GeoDataFrame, gps: gpd.GeoDataFrame) -> pd.DataFrame:
    """Count how many GPs fall within 1km of each ward's centroid."""
    wards = wards.to_crs(epsg=27700)
    gps = gps.to_crs(epsg=27700)

    ward_buffers = wards.copy()
    ward_buffers["geometry"] = ward_buffers.geometry.centroid
    ward_buffers["geometry"] = ward_buffers.geometry.buffer(1000)

    gp_matches = gpd.sjoin(gps, ward_buffers, how="inner", predicate="within")
    return gp_matches.groupby("WD23CD").size().reset_index(name="gp_count")


def prepare_fact_gp_counts(gp_counts: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Prepare GP counts for insertion, resolving each ward's current ward_id."""
    records = []
    for index, row in gp_counts.iterrows():
        current = get_current_ward(row["WD23CD"], engine)
        ward_id = current["ward_id"].iloc[0]
        records.append({
            "ward_id": ward_id,
            "gp_count": row["gp_count"],
            "measured_at": datetime.now(),
        })
    return pd.DataFrame(records)


def load_fact_gp_counts(fct: pd.DataFrame, engine: Engine) -> None:
    """Save GP-count measurements to the fact_gp_count table."""
    fct.to_sql("fact_gp_count", engine, if_exists="append", index=False)
    print(f"Saved {len(fct)} rows to fact_gp_count")


@dag(
    dag_id="gp_accessibility_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 21, tz="UTC"),
    catchup=False,
    tags=["gp_accessibility", "capstone"],
)
def gp_accessibility_pipeline():

    @task
    def run_extract_staging_wards():
        ward_boundaries = extract_wards("Newcastle upon Tyne")
        ward_boundaries = ward_boundaries.rename(
            columns={"WD23CD": "wd23cd", "WD23NM": "ward_name", "LAD23NM": "local_authority"}
        )
        ward_boundaries = ward_boundaries.rename_geometry("geom")
        ward_boundaries = ward_boundaries[["wd23cd", "ward_name", "local_authority", "geom"]]
        engine = get_engine()
        ward_boundaries.to_postgis("staging_wards", engine, if_exists="append", index=False)

    @task
    def run_extract_staging_gp_locations():
        gp_location = extract_gp_locations("Newcastle upon Tyne")
        gp_location = gp_location.to_crs(epsg=27700)
        gp_location = gp_location[["geometry"]]
        gp_location = gp_location.rename_geometry("geom")
        engine = get_engine()
        gp_location.to_postgis("staging_gp_locations", engine, if_exists="append", index=False)

    @task
    def run_syncing_ward():
        query = text("SELECT * FROM staging_wards")
        engine = get_engine()
        output = gpd.read_postgis(query, engine)
        output = output.rename(
            columns={"wd23cd": "WD23CD", "ward_name": "WD23NM", "local_authority": "LAD23NM"}
        )
        sync_all_wards(output, engine)

    @task
    def run_count_gp_facts():
        engine = get_engine()
        output = gpd.read_postgis(text("SELECT * FROM staging_wards"), engine)
        output = output.rename(columns={"wd23cd": "WD23CD"})
        output = output.rename_geometry("geometry")
        output_2 = gpd.read_postgis(text("SELECT * FROM staging_gp_locations"), engine)
        output_2 = output_2.rename_geometry("geometry")
        result = count_gps_per_ward(output, output_2)
        record = prepare_fact_gp_counts(result, engine)
        record["measured_at"] = record["measured_at"].astype(str)
        return record.to_dict(orient="records")

    @task
    def run_load_fact_gp_counts(data):
        record = pd.DataFrame(data)
        engine = get_engine()
        load_fact_gp_counts(record, engine)

    # --- wiring: your dependency graph, made real ---
    t1 = run_extract_staging_wards()
    t2 = run_extract_staging_gp_locations()
    t3 = run_syncing_ward()
    t4 = run_count_gp_facts()
    run_load_fact_gp_counts(t4)

    t1 >> t3
    t2 >> t4
    t3 >> t4


gp_accessibility_pipeline()
