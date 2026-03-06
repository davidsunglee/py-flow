"""
Mirror test for demo_platform_agents.py
==========================================
Verifies the full demo flow — 8-agent PlatformAgents team:

  1. OLTP Agent — create dataset, insert records, query
  2. Feed Agent — list symbols, check feed health
  3. Timeseries Agent — OHLCV bars, realized vol
  4. Lakehouse Agent — create table, run SQL
  5. Quant Agent — statistics, anomaly detection
  6. Document Agent — upload document, search
  7. Dashboard Agent — list/create ticking tables
  8. Query Agent — cross-store discovery

All agents use REAL platform services via conftest fixtures.
"""

import os
import tempfile
import textwrap

import pytest

from agents import PlatformAgents


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def infra(store_server, media_server, tsdb_server, market_data_server,
          lakehouse_server, streaming_server):
    """Register all services under 'pa-demo' alias — same as demo setup."""
    store_server.provision_user("pa_user", "pa_pw")
    store_server.register_alias("pa-demo")

    # Bootstrap media schemas
    from media.models import bootstrap_search_schema
    admin_conn = store_server.admin_conn()
    bootstrap_search_schema(admin_conn)
    admin_conn.close()

    media_server.register_alias("pa-demo")
    tsdb_server.register_alias("pa-demo")
    market_data_server.register_alias("pa-demo")
    lakehouse_server.register_alias("pa-demo")
    streaming_server.register_alias("pa-demo")

    return True


@pytest.fixture(scope="module")
def team(infra):
    """Build PlatformAgents team — same as demo."""
    return PlatformAgents(
        alias="pa-demo",
        user="pa_user",
        password="pa_pw",
    )


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoPlatformAgents:
    """Mirrors demo_platform_agents.py — 8-agent team."""

    def test_team_has_8_agents(self, team) -> None:
        """PlatformAgents initializes with 8 named agents."""
        assert len(team) == 8

    def test_agent_names(self, team) -> None:
        """All 8 expected agent names are present."""
        names = [name for name, _ in team]
        for expected in ("oltp", "feed", "timeseries", "lakehouse",
                         "quant", "document", "dashboard", "query"):
            assert expected in names, f"Missing agent: {expected}"

    def test_agents_are_runnable(self, team) -> None:
        """Each agent is runnable (has a .run method)."""
        for name, agent in team:
            assert hasattr(agent, "run"), f"Agent {name} has no run() method"

    # ── 1. OLTP Agent ────────────────────────────────────────────────

    def test_oltp_create_dataset(self, team) -> None:
        """OLTP agent: create a dataset."""
        resp = team.oltp.run(
            "Create a dataset called PATrade with fields: "
            "symbol (str), price (float), quantity (int), side (str)"
        )
        assert len(str(resp)) > 0

    def test_oltp_insert_records(self, team) -> None:
        """OLTP agent: insert records."""
        resp = team.oltp.run(
            "Insert these trades into PATrade: "
            "AAPL at 228.50 qty 100 buy, NVDA at 875.00 qty 50 sell"
        )
        assert len(str(resp)) > 0

    def test_oltp_query(self, team) -> None:
        """OLTP agent: query records back."""
        resp = team.oltp.run("Query all PATrade records")
        assert len(str(resp)) > 0

    # ── 2. Feed Agent ────────────────────────────────────────────────

    def test_feed_list_symbols(self, team) -> None:
        """Feed agent: list available symbols."""
        resp = team.feed.run("What symbols are available on the market data feed?")
        assert len(str(resp)) > 0

    def test_feed_health(self, team) -> None:
        """Feed agent: check feed health."""
        resp = team.feed.run("Is the market data feed healthy?")
        assert len(str(resp)) > 0

    # ── 3. Timeseries Agent ──────────────────────────────────────────

    def test_timeseries_bars(self, team) -> None:
        """Timeseries agent: get OHLCV bars."""
        resp = team.timeseries.run("Show me the latest 1-minute OHLCV bars for AAPL")
        assert len(str(resp)) > 0

    def test_timeseries_vol(self, team) -> None:
        """Timeseries agent: compute realized vol."""
        resp = team.timeseries.run("What is the realized volatility for NVDA?")
        assert len(str(resp)) > 0

    # ── 4. Lakehouse Agent ───────────────────────────────────────────

    def test_lakehouse_create_table(self, team) -> None:
        """Lakehouse agent: create analytical table from SQL."""
        resp = team.lakehouse.run(
            "Create a lakehouse table called pa_positions from this SQL: "
            "SELECT 'AAPL' as symbol, 228.5 as price, 500 as qty "
            "UNION ALL SELECT 'NVDA', 875.0, 200"
        )
        assert len(str(resp)) > 0

    def test_lakehouse_query(self, team) -> None:
        """Lakehouse agent: query the table."""
        resp = team.lakehouse.run(
            "What tables are in the lakehouse? Query pa_positions to show all rows."
        )
        assert len(str(resp)) > 0

    # ── 5. Quant Agent ───────────────────────────────────────────────

    def test_quant_stats(self, team) -> None:
        """Quant agent: run descriptive statistics."""
        resp = team.quant.run(
            "Run descriptive statistics on "
            "SELECT * FROM lakehouse.default.pa_positions, "
            "focusing on the price and qty columns"
        )
        assert len(str(resp)) > 0

    # ── 6. Document Agent ────────────────────────────────────────────

    def test_document_upload(self, team) -> None:
        """Document agent: upload a research note."""
        doc_path = os.path.join(tempfile.mkdtemp(), "test_note.txt")
        with open(doc_path, "w") as f:
            f.write(textwrap.dedent("""\
                NVDA Q4 Earnings Analysis — March 2024
                Revenue of $22.1B, up 265% YoY. Data center revenue $18.4B.
                Recommendation: Hold with position size cap at 20%.
            """))
        resp = team.document.run(
            f"Upload the document at {doc_path} with title 'NVDA Q4 Analysis' "
            "and tags 'nvda,earnings,ai'"
        )
        assert len(str(resp)) > 0

    def test_document_search(self, team) -> None:
        """Document agent: search for documents."""
        resp = team.document.run("Search for documents about NVIDIA earnings")
        assert len(str(resp)) > 0

    # ── 7. Dashboard Agent ───────────────────────────────────────────

    def test_dashboard_list(self, team) -> None:
        """Dashboard agent: list ticking tables."""
        resp = team.dashboard.run("What ticking tables are currently available?")
        assert len(str(resp)) > 0

    def test_dashboard_create(self, team) -> None:
        """Dashboard agent: create a ticking table."""
        resp = team.dashboard.run(
            "Create a ticking table called pa_prices with columns: "
            "symbol (str), price (float), volume (int)"
        )
        assert len(str(resp)) > 0

    # ── 8. Query Agent ───────────────────────────────────────────────

    def test_query_discovery(self, team) -> None:
        """Query agent: discover all datasets across platform stores."""
        resp = team.query.run("What datasets are available across all platform stores?")
        assert len(str(resp)) > 0

    # ── Multi-Agent Routing ──────────────────────────────────────────

    def test_router_dispatch(self, team) -> None:
        """Router dispatches prompt to correct agent."""
        resp = team.run("What symbols do we have in the OLTP store?")
        assert len(str(resp)) > 0
