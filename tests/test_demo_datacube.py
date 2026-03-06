"""
Mirror test for demo_datacube.py
==================================
Verifies the full demo flow:
  1. Ingest data into Lakehouse (Iceberg on S3)
  2. Datacube queries: flat, GROUP BY, HPivot, filter, drilldown, extend
  3. Full pipeline: filter + extend + group + pivot + sort + limit

Uses small local data instead of NYC taxi URLs for speed.
"""

import pytest

from lakehouse import Lakehouse


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def lh(lakehouse_server):
    """Lakehouse client connected to shared stack."""
    lakehouse_server.register_alias("dc-demo")
    inst = Lakehouse("dc-demo")
    yield inst
    inst.close()


@pytest.fixture(scope="module")
def seeded(lh):
    """Ingest test data mirroring the taxi schema."""
    data = [
        {"payment_type": 1, "rate_code": 1, "fare_amount": 25.0, "tip_amount": 5.0,
         "tolls_amount": 0.0, "total_amount": 30.0, "trip_distance": 5.2, "passenger_count": 1},
        {"payment_type": 1, "rate_code": 1, "fare_amount": 40.0, "tip_amount": 8.0,
         "tolls_amount": 2.5, "total_amount": 50.5, "trip_distance": 12.0, "passenger_count": 2},
        {"payment_type": 1, "rate_code": 2, "fare_amount": 52.0, "tip_amount": 10.0,
         "tolls_amount": 6.5, "total_amount": 68.5, "trip_distance": 18.0, "passenger_count": 1},
        {"payment_type": 2, "rate_code": 1, "fare_amount": 15.0, "tip_amount": 0.0,
         "tolls_amount": 0.0, "total_amount": 15.0, "trip_distance": 3.0, "passenger_count": 3},
        {"payment_type": 2, "rate_code": 1, "fare_amount": 20.0, "tip_amount": 0.0,
         "tolls_amount": 0.0, "total_amount": 20.0, "trip_distance": 4.5, "passenger_count": 1},
        {"payment_type": 3, "rate_code": 1, "fare_amount": 10.0, "tip_amount": 0.0,
         "tolls_amount": 0.0, "total_amount": 10.0, "trip_distance": 2.0, "passenger_count": 1},
    ]
    n = lh.ingest("dc_taxi", data, mode="append")
    return n


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoDatacube:
    """Mirrors demo_datacube.py flow."""

    def test_ingest_rows(self, seeded) -> None:
        """Data was ingested."""
        assert seeded == 6

    def test_flat_count(self, lh, seeded) -> None:
        """Q1: SELECT count(*)."""
        rows = lh.query("SELECT count(*) as cnt FROM lakehouse.default.dc_taxi")
        assert rows[0]["cnt"] == 6

    def test_datacube_creation(self, lh, seeded) -> None:
        """Datacube discovers columns from Iceberg table."""
        dc = lh.datacube("dc_taxi")
        col_names = [c.name for c in dc.snapshot.columns]
        assert "fare_amount" in col_names
        assert "payment_type" in col_names

    def test_group_by_payment_type(self, lh, seeded) -> None:
        """Q2: GROUP BY payment_type."""
        dc = lh.datacube("dc_taxi").set_group_by("payment_type")
        df = dc.query_df()
        assert len(df) == 3  # types 1, 2, 3

    def test_two_level_group_by(self, lh, seeded) -> None:
        """Q3: GROUP BY payment_type × rate_code."""
        dc = lh.datacube("dc_taxi").set_group_by("payment_type", "rate_code")
        df = dc.query_df()
        assert len(df) >= 3

    def test_hpivot(self, lh, seeded) -> None:
        """Q4: rate_code rows × payment_type columns."""
        dc = (lh.datacube("dc_taxi")
              .set_group_by("rate_code")
              .set_pivot_by("payment_type"))
        df = dc.query_df()
        assert len(df) >= 1
        assert len(df.columns) > 2  # rate_code + at least 2 pivoted columns

    def test_filter_and_group(self, lh, seeded) -> None:
        """Q5: Credit card trips only, grouped by rate_code, avg aggregation."""
        dc = (lh.datacube("dc_taxi")
              .add_filter("payment_type", "eq", 1)
              .set_group_by("rate_code")
              .set_column("fare_amount", aggregate_operator="avg")
              .set_column("tip_amount", aggregate_operator="avg"))
        df = dc.query_df()
        assert len(df) == 2  # rate_code 1 and 2

    def test_drilldown(self, lh, seeded) -> None:
        """Q6: Drilldown payment_type → rate_code."""
        dc = lh.datacube("dc_taxi").set_group_by("payment_type", "rate_code")
        df_top = dc.query_df()
        assert len(df_top) >= 3

        dc_drill = dc.drill_down(payment_type=1)
        df_drill = dc_drill.query_df()
        assert len(df_drill) == 2  # rate_code 1 and 2 for payment_type=1

    def test_leaf_extend(self, lh, seeded) -> None:
        """Q7: Computed column tip_pct = tip/fare."""
        dc = (lh.datacube("dc_taxi")
              .add_leaf_extend("tip_pct",
                               "CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END")
              .set_group_by("payment_type")
              .set_column("tip_pct", aggregate_operator="avg"))
        df = dc.query_df()
        assert len(df) == 3
        assert "tip_pct" in df.columns

    def test_full_pipeline(self, lh, seeded) -> None:
        """Q8: Full pipeline — filter + extend + group + pivot + sort + limit."""
        dc = (lh.datacube("dc_taxi")
              .add_filter("payment_type", "in", [1, 2])
              .add_leaf_extend("tip_pct",
                               "CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END")
              .set_group_by("rate_code")
              .set_pivot_by("payment_type")
              .set_column("tip_pct", aggregate_operator="avg")
              .set_sort(("rate_code", False))
              .set_limit(10))
        df = dc.query_df()
        assert len(df) >= 1
        assert len(df.columns) >= 2
