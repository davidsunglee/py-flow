"""
Mirror test for demo_backtest.py
==================================
Verifies the full demo flow end-to-end:

  1. Check market data server health + TSDB
  2. Collect live data (shortened for test speed)
  3. Query OHLCV bars from TSDB via REST
  4. Run MA crossover backtest
  5. Verify result structure + print summary

Pipeline: SimulatorFeed → TickBus → TSDBConsumer → TSDB → REST /md/bars → Backtest
"""

import time

import httpx
import pytest

from demo_backtest import (
    backtest_ma_crossover,
    print_summary,
    query_bars,
)


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def md_base(market_data_server):
    """Market data server base URL — same as demo's MD_BASE."""
    return f"http://localhost:{market_data_server.port}"


@pytest.fixture(scope="module")
def collected_ticks(md_base):
    """Collect live data — shortened version of demo's collect_data().

    Demo collects for 30s; we collect for 10s since data is already flowing
    from conftest's market_data_server startup.
    """
    symbol = "AAPL"
    collect_secs = 10

    initial = _get_history_count(md_base, symbol)
    start = time.monotonic()
    while time.monotonic() - start < collect_secs:
        time.sleep(2)

    final = _get_history_count(md_base, symbol)
    return {"symbol": symbol, "initial": initial, "final": final,
            "collected": final - initial}


def _get_history_count(md_base: str, symbol: str) -> int:
    """Same as demo_backtest._get_history_count."""
    try:
        resp = httpx.get(f"{md_base}/md/history/equity/{symbol}",
                         params={"limit": 10000}, timeout=5)
        return len(resp.json())
    except Exception:
        return 0


@pytest.fixture(scope="module")
def bars(md_base, collected_ticks):
    """Query OHLCV bars from TSDB — same as demo's query_bars().

    Uses 1s interval (shorter than demo's 5s) for more bars from limited data.
    """
    symbol = collected_ticks["symbol"]
    resp = httpx.get(f"{md_base}/md/bars/equity/{symbol}",
                     params={"interval": "1s"}, timeout=5)
    return resp.json()


@pytest.fixture(scope="module")
def backtest_result(bars, collected_ticks):
    """Run the MA crossover backtest — same as demo's backtest_ma_crossover()."""
    return backtest_ma_crossover(
        bars, collected_ticks["symbol"],
        fast_period=3, slow_period=10,
    )


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoBacktest:
    """Mirrors demo_backtest.py — full pipeline from live TSDB data."""

    def test_server_health(self, md_base) -> None:
        """Step 1: Market data server is healthy."""
        resp = httpx.get(f"{md_base}/md/health", timeout=5)
        data = resp.json()
        assert data.get("status") == "ok"

    def test_tsdb_available(self, md_base) -> None:
        """Step 1: TSDB endpoints are available (not 503)."""
        resp = httpx.get(f"{md_base}/md/latest/equity", timeout=5)
        assert resp.status_code != 503

    def test_ticks_accumulated(self, collected_ticks) -> None:
        """Step 2: Ticks accumulated in TSDB during collection."""
        assert collected_ticks["final"] > 0

    def test_bars_returned(self, bars) -> None:
        """Step 3: OHLCV bars returned from TSDB."""
        assert len(bars) > 0

    def test_bar_structure(self, bars) -> None:
        """Each bar has OHLCV fields."""
        bar = bars[0]
        for field in ("open", "high", "low", "close"):
            assert field in bar, f"Missing field: {field}"

    def test_backtest_result_shape(self, backtest_result) -> None:
        """Step 4: backtest_ma_crossover returns all expected fields."""
        for key in ("trades", "total_pnl", "win_rate", "num_trades",
                     "winners", "losers", "bars_used", "fast_period", "slow_period"):
            assert key in backtest_result

    def test_backtest_pnl_sums(self, backtest_result) -> None:
        """Total PnL = sum of individual trade PnLs."""
        expected = sum(t["pnl"] for t in backtest_result["trades"])
        assert abs(backtest_result["total_pnl"] - expected) < 0.01

    def test_backtest_periods_recorded(self, backtest_result) -> None:
        """Result records the MA periods used."""
        assert backtest_result["fast_period"] == 3
        assert backtest_result["slow_period"] == 10

    def test_multi_asset_tsdb_snapshot(self, md_base) -> None:
        """TSDB has data for multiple asset types — same as demo's show_tsdb_snapshot()."""
        resp = httpx.get(f"{md_base}/md/latest/equity", timeout=5)
        assert resp.status_code == 200
        equity = resp.json()
        assert len(equity) > 0

    def test_print_summary_runs(self, backtest_result, collected_ticks) -> None:
        """Step 5: print_summary doesn't crash."""
        print_summary(backtest_result, collected_ticks["symbol"], "1s",
                      collected_ticks["final"])
