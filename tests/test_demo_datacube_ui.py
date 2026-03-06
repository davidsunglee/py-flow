"""
Mirror test for demo_datacube_ui.py
====================================
Verifies the full demo flow:
  1. Load Parquet data into DuckDB (from HTTPS)
  2. Create Datacube from DuckDB connection
  3. Inspect columns, dimensions, measures
"""

import duckdb
import pytest

from datacube import Datacube

TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


@pytest.fixture(scope="module")
def taxi_conn():
    """DuckDB connection with taxi data loaded — same as demo."""
    conn = duckdb.connect()
    conn.execute(f"""
        CREATE TABLE yellow_taxi AS
        SELECT
            VendorID::VARCHAR           AS vendor,
            tpep_pickup_datetime        AS pickup_time,
            tpep_dropoff_datetime       AS dropoff_time,
            passenger_count::INTEGER    AS passengers,
            trip_distance::DOUBLE       AS distance,
            RatecodeID::VARCHAR         AS rate_code,
            PULocationID::VARCHAR       AS pickup_zone,
            DOLocationID::VARCHAR       AS dropoff_zone,
            payment_type::VARCHAR       AS payment_type,
            fare_amount::DOUBLE         AS fare,
            extra::DOUBLE               AS extra,
            mta_tax::DOUBLE             AS mta_tax,
            tip_amount::DOUBLE          AS tip,
            tolls_amount::DOUBLE        AS tolls,
            total_amount::DOUBLE        AS total,
            congestion_surcharge::DOUBLE AS congestion
        FROM read_parquet('{TAXI_URL}')
    """)
    yield conn
    conn.close()


class TestDemoDatacubeUI:
    """Mirrors the demo_datacube_ui.py flow."""

    def test_row_count(self, taxi_conn) -> None:
        """Verify taxi data was loaded (should be several million rows)."""
        row = taxi_conn.execute("SELECT count(*) FROM yellow_taxi").fetchone()
        assert row is not None
        assert row[0] > 1_000_000

    def test_datacube_creation(self, taxi_conn) -> None:
        """Datacube can be created from a DuckDB connection."""
        dc = Datacube(taxi_conn, source_name="yellow_taxi")
        assert dc.snapshot is not None
        assert len(dc.snapshot.columns) > 0

    def test_columns_match_demo(self, taxi_conn) -> None:
        """Column names match the demo's SELECT aliases."""
        dc = Datacube(taxi_conn, source_name="yellow_taxi")
        col_names = [c.name for c in dc.snapshot.columns]
        for expected in ["vendor", "fare", "tip", "total", "distance", "passengers"]:
            assert expected in col_names, f"Missing column: {expected}"

    def test_dimensions_available(self, taxi_conn) -> None:
        """Datacube discovers dimension columns (strings)."""
        dc = Datacube(taxi_conn, source_name="yellow_taxi")
        dims = dc.available_dimensions()
        assert len(dims) > 0
        assert "vendor" in dims or "payment_type" in dims

    def test_measures_available(self, taxi_conn) -> None:
        """Datacube discovers measure columns (numerics)."""
        dc = Datacube(taxi_conn, source_name="yellow_taxi")
        measures = dc.available_measures()
        assert len(measures) > 0
        assert "fare" in measures or "total" in measures
