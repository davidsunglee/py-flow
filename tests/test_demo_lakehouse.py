"""
Mirror test for demo_lakehouse.py
====================================
Verifies the core demo flow:

  1. Start Lakehouse stack
  2. Ingest data (Storable-like dicts)
  3. Query via DuckDB/Iceberg
  4. List tables and row counts

Note: The TSDB tick sync path is covered by test_lakehouse_integration.py.
This test focuses on the Lakehouse ingest + query API that the demo showcases.
"""

import pytest

from lakehouse import Lakehouse


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def lh(lakehouse_server):
    """Lakehouse client."""
    lakehouse_server.register_alias("lh-demo")
    inst = Lakehouse("lh-demo")
    yield inst
    inst.close()


@pytest.fixture(scope="module")
def seeded(lh):
    """Seed data mirroring demo_lakehouse.py's Trade + Order objects."""
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NVDA", "JPM"]
    trades = [
        {"symbol": sym, "quantity": (i + 1) * 100, "price": 150.0 + i * 10, "side": "BUY"}
        for i, sym in enumerate(symbols)
    ]
    n_trades = lh.ingest("lh_demo_trades", trades, mode="append")

    orders = [
        {"symbol": sym, "quantity": (i + 1) * 50, "limit_price": 148.0 + i * 10, "order_type": "LIMIT"}
        for i, sym in enumerate(symbols)
    ]
    n_orders = lh.ingest("lh_demo_orders", orders, mode="append")

    return {"trades": n_trades, "orders": n_orders}


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoLakehouse:
    """Mirrors demo_lakehouse.py flow — seed + query."""

    def test_seed_trades(self, seeded) -> None:
        """8 trade objects ingested."""
        assert seeded["trades"] == 8

    def test_seed_orders(self, seeded) -> None:
        """8 order objects ingested."""
        assert seeded["orders"] == 8

    def test_query_trades_via_duckdb(self, lh, seeded) -> None:
        """Trades queryable via DuckDB."""
        rows = lh.query("SELECT * FROM lakehouse.default.lh_demo_trades")
        assert len(rows) == 8

    def test_query_orders_via_duckdb(self, lh, seeded) -> None:
        """Orders queryable via DuckDB."""
        rows = lh.query("SELECT * FROM lakehouse.default.lh_demo_orders")
        assert len(rows) == 8

    def test_list_tables(self, lh, seeded) -> None:
        """All demo tables visible."""
        tables = lh.tables()
        assert "lh_demo_trades" in tables
        assert "lh_demo_orders" in tables

    def test_row_counts(self, lh, seeded) -> None:
        """Row counts match seeded data."""
        assert lh.row_count("lh_demo_trades") == 8
        assert lh.row_count("lh_demo_orders") == 8

    def test_query_by_symbol(self, lh, seeded) -> None:
        """Can query by symbol — same as demo's SQL exploration."""
        rows = lh.query(
            "SELECT symbol, quantity, price FROM lakehouse.default.lh_demo_trades WHERE symbol = 'AAPL'"
        )
        assert len(rows) == 1
        assert rows[0]["symbol"] == "AAPL"
