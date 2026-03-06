"""
Mirror test for demo_ir_swap.py
=================================
Verifies the full demo flow — reactive IRS pricing cascade:

  1. FXSpot: batch_update(bid, ask) → @computed mid → @effect → DH push
  2. YieldCurvePoint: @computed rate (cross-entity: reads fx_ref.mid)
     → @computed discount_factor → @effect → DH push
  3. InterestRateSwap: @computed float_rate (from curve_ref.rate)
     → @computed npv, dv01, pnl_status → @effect → DH push
  4. SwapPortfolio: @computed total_npv (reads child swap NPVs)
     → @effect → DH push

Tests the reactive cascade WITHOUT the WS consumer — just batch_update.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from reactive.computed import computed, effect
from streaming import agg, flush, get_tables, ticking

from store import Storable


# ── Reactive domain models — same as demo ────────────────────────────────

_curve_publish_queue: deque = deque()


@ticking
@dataclass
class IRSFXSpot(Storable):
    __key__ = "pair"
    pair: str = ""
    bid: float = 0.0
    ask: float = 0.0
    currency: str = ""

    @computed
    def mid(self):
        return (self.bid + self.ask) / 2

    @computed
    def spread_pips(self):
        return (self.ask - self.bid) * 10000

    @effect("mid")
    def on_mid(self, value):
        self.tick()


@ticking(exclude={"base_rate", "sensitivity", "fx_base_mid"})
@dataclass
class IRSCurvePoint(Storable):
    __key__ = "label"
    label: str = ""
    tenor_years: float = 0.0
    base_rate: float = 0.0
    sensitivity: float = 0.5
    currency: str = "USD"
    fx_ref: object = None
    fx_base_mid: float = 0.0

    @computed
    def rate(self):
        if self.fx_ref is None:
            return self.base_rate
        fx_base = self.fx_base_mid
        if fx_base == 0.0:
            return self.base_rate
        pct_move = (self.fx_ref.mid - fx_base) / fx_base
        return max(0.0001, self.base_rate + self.sensitivity * pct_move)

    @computed
    def discount_factor(self):
        return 1.0 / (1.0 + self.rate) ** self.tenor_years

    @effect("rate")
    def on_rate(self, value):
        self.tick()
        _curve_publish_queue.append({
            "label": self.label, "rate": self.rate,
            "discount_factor": self.discount_factor,
        })


@ticking
@dataclass
class IRSwap(Storable):
    __key__ = "symbol"
    symbol: str = ""
    notional: float = 0.0
    fixed_rate: float = 0.0
    tenor_years: float = 0.0
    currency: str = "USD"
    curve_ref: object = None

    @computed
    def float_rate(self):
        if self.curve_ref is None:
            return 0.0
        return self.curve_ref.rate

    @computed
    def fixed_leg_pv(self):
        df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        return self.notional * self.fixed_rate * self.tenor_years * df

    @computed
    def float_leg_pv(self):
        df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        return self.notional * self.float_rate * self.tenor_years * df

    @computed
    def npv(self):
        float_df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        fixed_df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        float_pv = self.notional * self.float_rate * self.tenor_years * float_df
        fixed_pv = self.notional * self.fixed_rate * self.tenor_years * fixed_df
        return float_pv - fixed_pv

    @computed
    def dv01(self):
        return self.notional * self.tenor_years * 0.0001

    @computed
    def pnl_status(self) -> str:
        if self.npv > 0:
            return "PROFIT"
        if self.npv < 0:
            return "LOSS"
        return "FLAT"

    @effect("npv")
    def on_npv(self, value):
        self.tick()


@ticking
@dataclass
class IRSPortfolio(Storable):
    __key__ = "name"
    name: str = ""
    swaps: list = field(default_factory=list)

    @computed
    def total_npv(self):
        return sum(s.npv for s in self.swaps) if self.swaps else 0.0

    @computed
    def total_dv01(self):
        return sum(s.dv01 for s in self.swaps) if self.swaps else 0.0

    @computed
    def swap_count(self) -> int:
        return len(self.swaps) if self.swaps else 0

    @effect("total_npv")
    def on_total_npv(self, value):
        self.tick()


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def reactive_graph(streaming_server):
    """Build the reactive graph — same as demo's Section 4."""
    # FX spots
    eur_usd = IRSFXSpot(pair="EUR/USD", bid=1.0850, ask=1.0855, currency="USD")
    usd_jpy = IRSFXSpot(pair="USD/JPY", bid=149.50, ask=149.60, currency="JPY")

    # Yield curve points — cross-entity: reads fx_ref.mid
    usd_5y = IRSCurvePoint(
        label="USD_5Y", tenor_years=5.0, base_rate=0.0410,
        sensitivity=0.5, currency="USD",
        fx_ref=eur_usd, fx_base_mid=eur_usd.mid,
    )
    usd_10y = IRSCurvePoint(
        label="USD_10Y", tenor_years=10.0, base_rate=0.0395,
        sensitivity=0.5, currency="USD",
        fx_ref=eur_usd, fx_base_mid=eur_usd.mid,
    )
    jpy_5y = IRSCurvePoint(
        label="JPY_5Y", tenor_years=5.0, base_rate=0.005,
        sensitivity=0.5, currency="JPY",
        fx_ref=usd_jpy, fx_base_mid=usd_jpy.mid,
    )

    # IRS — cross-entity: reads curve_ref.rate
    swap_usd_5y = IRSwap(
        symbol="USD-5Y-A", notional=50_000_000, fixed_rate=0.0400,
        tenor_years=5.0, currency="USD", curve_ref=usd_5y,
    )
    swap_usd_10y = IRSwap(
        symbol="USD-10Y", notional=100_000_000, fixed_rate=0.0395,
        tenor_years=10.0, currency="USD", curve_ref=usd_10y,
    )
    swap_jpy_5y = IRSwap(
        symbol="JPY-5Y", notional=5_000_000_000, fixed_rate=0.005,
        tenor_years=5.0, currency="JPY", curve_ref=jpy_5y,
    )

    # Portfolio — cross-entity: reads child swap NPVs
    all_portfolio = IRSPortfolio(
        name="ALL", swaps=[swap_usd_5y, swap_usd_10y, swap_jpy_5y],
    )

    # Publish to DH
    tables = get_tables()
    for name, tbl in tables.items():
        tbl.publish(f"irs_{name}")

    swap_summary = IRSwap._ticking_live.agg_by([
        agg.sum(["TotalNPV=npv", "TotalDV01=dv01"]),
        agg.count("NumSwaps"),
    ])
    swap_summary.publish("irs_swap_summary")

    flush()
    _curve_publish_queue.clear()

    return {
        "eur_usd": eur_usd, "usd_jpy": usd_jpy,
        "usd_5y": usd_5y, "usd_10y": usd_10y, "jpy_5y": jpy_5y,
        "swap_usd_5y": swap_usd_5y, "swap_usd_10y": swap_usd_10y,
        "swap_jpy_5y": swap_jpy_5y,
        "portfolio": all_portfolio,
    }


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoIRSwap:
    """Mirrors demo_ir_swap.py — fully reactive IRS cascade."""

    def test_fx_spot_mid(self, reactive_graph) -> None:
        """FXSpot: @computed mid = (bid + ask) / 2."""
        eur = reactive_graph["eur_usd"]
        assert abs(eur.mid - (1.0850 + 1.0855) / 2) < 1e-6

    def test_fx_spot_spread(self, reactive_graph) -> None:
        """FXSpot: @computed spread_pips."""
        eur = reactive_graph["eur_usd"]
        assert eur.spread_pips > 0

    def test_curve_rate_at_inception(self, reactive_graph) -> None:
        """YieldCurvePoint: rate ≈ base_rate at inception (FX unchanged)."""
        usd_5y = reactive_graph["usd_5y"]
        assert abs(usd_5y.rate - 0.0410) < 0.001

    def test_curve_discount_factor(self, reactive_graph) -> None:
        """YieldCurvePoint: discount_factor = 1/(1+rate)^tenor."""
        usd_5y = reactive_graph["usd_5y"]
        expected_df = 1.0 / (1.0 + usd_5y.rate) ** 5.0
        assert abs(usd_5y.discount_factor - expected_df) < 1e-6

    def test_swap_float_rate(self, reactive_graph) -> None:
        """IRS: float_rate = curve_ref.rate (cross-entity)."""
        swap = reactive_graph["swap_usd_5y"]
        usd_5y = reactive_graph["usd_5y"]
        assert abs(swap.float_rate - usd_5y.rate) < 1e-10

    def test_swap_npv(self, reactive_graph) -> None:
        """IRS: NPV is computed from fixed/floating leg PVs."""
        swap = reactive_graph["swap_usd_5y"]
        assert isinstance(swap.npv, float)

    def test_swap_dv01(self, reactive_graph) -> None:
        """IRS: DV01 = notional × tenor × 0.0001."""
        swap = reactive_graph["swap_usd_5y"]
        expected = 50_000_000 * 5.0 * 0.0001
        assert abs(swap.dv01 - expected) < 1e-6

    def test_swap_pnl_status(self, reactive_graph) -> None:
        """IRS: pnl_status is PROFIT, LOSS, or FLAT."""
        swap = reactive_graph["swap_usd_5y"]
        assert swap.pnl_status in ("PROFIT", "LOSS", "FLAT")

    def test_portfolio_total_npv(self, reactive_graph) -> None:
        """SwapPortfolio: total_npv = sum of child swap NPVs."""
        port = reactive_graph["portfolio"]
        expected = sum(s.npv for s in port.swaps)
        assert abs(port.total_npv - expected) < 1e-6

    def test_portfolio_swap_count(self, reactive_graph) -> None:
        """SwapPortfolio: swap_count = 3."""
        port = reactive_graph["portfolio"]
        assert port.swap_count == 3

    def test_fx_update_cascades(self, reactive_graph) -> None:
        """Cascade: FX bid/ask change → curve rate → swap float_rate → NPV."""
        eur = reactive_graph["eur_usd"]
        usd_5y = reactive_graph["usd_5y"]
        swap = reactive_graph["swap_usd_5y"]

        old_rate = usd_5y.rate
        old_npv = swap.npv

        # Simulate FX tick — same as demo's single batch_update()
        eur.batch_update(bid=1.0900, ask=1.0905)
        flush()

        # Rate should change (FX moved)
        assert usd_5y.rate != old_rate
        # NPV should also change (float_rate changed)
        assert swap.npv != old_npv

    def test_curve_publish_queue(self, reactive_graph) -> None:
        """@effect on rate enqueues CurveTick — same as demo's _curve_publish_queue."""
        # After the FX update in test above, queue should have entries
        assert len(_curve_publish_queue) > 0

    def test_portfolio_reacts_to_fx(self, reactive_graph) -> None:
        """Portfolio total_npv reacts to FX change via cascade."""
        port = reactive_graph["portfolio"]
        # After the FX update, portfolio NPV should match sum of swaps
        expected = sum(s.npv for s in port.swaps)
        assert abs(port.total_npv - expected) < 1e-6
