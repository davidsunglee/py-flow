"""
Finance/trading test suite for the reactive computation layer.

Tests realistic pricing, risk, and P&L computations using domain objects
with @computed decorators for pure OO reactive properties.
"""

import math
import pytest
from dataclasses import dataclass, field
from typing import Optional

from store.base import Storable
from reactive.expr import Const, Field, BinOp, Func, If, Coalesce, IsNull, from_json
from reactive.computed import computed, effect


# ---------------------------------------------------------------------------
# Finance domain models with @computed
# ---------------------------------------------------------------------------

@dataclass
class MarketData(Storable):
    """Live market data tick."""
    symbol: str = ""
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    volume: int = 0

    @computed
    def mid(self):
        return (self.bid + self.ask) / 2

    @computed
    def spread(self):
        return self.ask - self.bid

    @computed
    def spread_bps(self):
        mid = (self.bid + self.ask) / 2
        return (self.ask - self.bid) / mid * 10000


@dataclass
class Position(Storable):
    """A portfolio position."""
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0
    side: str = "LONG"

    @computed
    def unrealized_pnl(self):
        return (self.current_price - self.avg_cost) * self.quantity

    @computed
    def pnl(self):
        return (self.current_price - self.avg_cost) * self.quantity

    @computed
    def pnl_pct(self):
        return (self.current_price - self.avg_cost) / self.avg_cost * 100

    @computed
    def market_value(self):
        return self.current_price * self.quantity

    @computed
    def notional(self):
        return self.quantity * self.current_price

    @computed
    def weight_pct(self):
        return self.quantity * self.current_price / 1_000_000 * 100

    @computed
    def stop_loss_status(self):
        pnl = (self.current_price - self.avg_cost) * self.quantity
        if pnl < -5000:
            return "STOP_LOSS"
        return "OK"

    @computed
    def limit_status(self):
        notional = self.quantity * self.current_price
        if notional > 50000:
            return "BREACH"
        return "OK"


@dataclass
class Option(Storable):
    """A vanilla equity option."""
    symbol: str = ""
    underlying_price: float = 0.0
    strike: float = 0.0
    time_to_expiry: float = 0.0
    volatility: float = 0.0
    risk_free_rate: float = 0.0
    option_type: str = "CALL"

    @computed
    def moneyness(self):
        return self.underlying_price / self.strike

    @computed
    def intrinsic_call(self):
        if self.underlying_price > self.strike:
            return self.underlying_price - self.strike
        return 0

    @computed
    def time_value_proxy(self):
        return self.volatility * math.sqrt(self.time_to_expiry) * self.underlying_price


@dataclass
class FXRate(Storable):
    """Foreign exchange rate."""
    pair: str = ""
    rate: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    amount: float = 0.0

    @computed
    def eur_value(self):
        return self.amount / self.rate if self.rate else 0

    @computed
    def spread_pips(self):
        return (self.ask - self.bid) * 10000


@dataclass
class Bond(Storable):
    """A fixed income bond."""
    isin: str = ""
    face_value: float = 1000.0
    coupon_rate: float = 0.0
    yield_to_maturity: float = 0.0
    years_to_maturity: float = 0.0
    price: float = 0.0

    @computed
    def current_yield(self):
        return (self.coupon_rate * self.face_value) / self.price

    @computed
    def annual_coupon(self):
        return self.face_value * self.coupon_rate

    @computed
    def duration_proxy(self):
        return self.years_to_maturity * (1 - self.coupon_rate)

    @computed
    def duration(self):
        return self.years_to_maturity * (1 - self.coupon_rate)

    @computed
    def price_impact_10bp(self):
        dur = self.years_to_maturity * (1 - self.coupon_rate)
        return -self.price * dur * 0.001

    @computed
    def pricing_label(self):
        if self.price > self.face_value:
            return "PREMIUM"
        if self.price < self.face_value:
            return "DISCOUNT"
        return "PAR"


@dataclass
class RiskPosition(Storable):
    """Risk position for VaR computations."""
    notional: float = 0.0
    daily_vol: float = 0.0
    z_score: float = 1.645

    @computed
    def var_1d(self):
        return self.notional * self.daily_vol * self.z_score

    @computed
    def var_10d(self):
        return self.notional * self.daily_vol * self.z_score * math.sqrt(10)


@dataclass
class TradingSignal(Storable):
    """Trading signal with computed score and confidence."""
    symbol: str = ""
    direction: str = "LONG"
    strength: float = 0.0
    model_name: str = ""

    @computed
    def score(self):
        if "LONG" in self.direction:
            return self.strength
        return -self.strength

    @computed
    def confidence(self):
        if self.strength > 0.75:
            return "HIGH"
        if self.strength > 0.5:
            return "MEDIUM"
        return "LOW"

    @computed
    def model_upper(self):
        return self.model_name.upper()

    @computed
    def is_momentum(self):
        return self.model_name.startswith("momentum")


# Effect test helpers
_pnl_alerts = []
_mid_ticks = []


@dataclass
class PositionWithPnLEffect(Storable):
    """Position with @effect on pnl for testing alerts."""
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0
    side: str = "LONG"

    @computed
    def pnl(self):
        return (self.current_price - self.avg_cost) * self.quantity

    @effect("pnl")
    def on_pnl(self, value):
        _pnl_alerts.append(value)


@dataclass
class MarketDataWithMidEffect(Storable):
    """MarketData with @effect on mid for testing tick events."""
    symbol: str = ""
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    volume: int = 0

    @computed
    def mid(self):
        return (self.bid + self.ask) / 2

    @effect("mid")
    def on_mid(self, value):
        _mid_ticks.append(value)


# ===========================================================================
# Pricing tests
# ===========================================================================

class TestMarketDataMidPrice:
    """Mid-price = (bid + ask) / 2"""

    def test_mid_price_computation(self):
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.00, last=227.75)
        assert md.mid == 227.75

    def test_mid_price_updates_on_tick(self):
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.00)
        md.batch_update(bid=229.00, ask=229.50)
        assert md.mid == 229.25

    def test_spread_computation(self):
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.00)
        assert md.spread == 0.50

    def test_spread_bps(self):
        """Spread in basis points = (ask - bid) / mid * 10000"""
        md = MarketData(symbol="AAPL", bid=227.50, ask=228.50)
        expected = (228.50 - 227.50) / 228.0 * 10000
        assert abs(md.spread_bps - expected) < 0.01

    def test_to_sql(self):
        mid_expr = (Field("bid") + Field("ask")) / Const(2)
        sql = mid_expr.to_sql("data")
        assert "(data->>'bid')::float" in sql
        assert "(data->>'ask')::float" in sql

    def test_to_pure(self):
        mid_expr = (Field("bid") + Field("ask")) / Const(2)
        pure = mid_expr.to_pure("$tick")
        assert "$tick.bid" in pure
        assert "$tick.ask" in pure


class TestPositionPnL:
    """Unrealized P&L = (current_price - avg_cost) * quantity"""

    def test_long_pnl_profit(self):
        pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0, side="LONG")
        assert pos.unrealized_pnl == 1000.0

    def test_long_pnl_loss(self):
        pos = Position(symbol="AAPL", quantity=100, avg_cost=230.0, current_price=220.0)
        assert pos.unrealized_pnl == -1000.0

    def test_pnl_updates_on_price_change(self):
        pos = Position(symbol="TSLA", quantity=50, avg_cost=180.0, current_price=180.0)
        assert pos.unrealized_pnl == 0.0

        pos.current_price = 200.0
        assert pos.unrealized_pnl == 1000.0

        pos.current_price = 170.0
        assert pos.unrealized_pnl == -500.0

    def test_pnl_percentage(self):
        pos = Position(symbol="MSFT", quantity=200, avg_cost=400.0, current_price=420.0)
        assert pos.pnl_pct == 5.0

    def test_market_value(self):
        pos = Position(symbol="GOOG", quantity=50, avg_cost=170.0, current_price=175.0)
        assert pos.market_value == 8750.0

    def test_pnl_sql(self):
        """@computed generates Expr for SQL compilation."""
        expr = Position.unrealized_pnl.expr
        assert expr is not None
        sql = expr.to_sql("data")
        assert "(data->>'current_price')::float" in sql
        assert "(data->>'avg_cost')::float" in sql
        assert "(data->>'quantity')::float" in sql

    def test_pnl_pure(self):
        """@computed generates Expr for Legend Pure compilation."""
        expr = Position.unrealized_pnl.expr
        assert expr is not None
        pure = expr.to_pure("$pos")
        assert "$pos.current_price" in pure
        assert "$pos.avg_cost" in pure
        assert "$pos.quantity" in pure


class TestOptionPricing:
    """Simplified Black-Scholes-style option computations."""

    def test_itm_call_intrinsic(self):
        opt = Option(underlying_price=110.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        assert opt.intrinsic_call == 10.0

    def test_otm_call_intrinsic(self):
        opt = Option(underlying_price=90.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        assert opt.intrinsic_call == 0

    def test_atm_moneyness(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        assert opt.moneyness == 1.0

    def test_itm_moneyness(self):
        opt = Option(underlying_price=120.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        assert opt.moneyness == 1.2

    def test_time_value_proxy(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=1.0, volatility=0.25)
        expected = 0.25 * math.sqrt(1.0) * 100.0
        assert abs(opt.time_value_proxy - expected) < 0.001

    def test_option_reacts_to_spot_move(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=0.5, volatility=0.2)
        assert opt.intrinsic_call == 0

        opt.underlying_price = 115.0
        assert opt.intrinsic_call == 15.0
        assert opt.moneyness == 1.15

    def test_option_reacts_to_vol_change(self):
        opt = Option(underlying_price=100.0, strike=100.0, time_to_expiry=1.0, volatility=0.20)
        tv_before = opt.time_value_proxy

        opt.volatility = 0.40
        tv_after = opt.time_value_proxy

        assert tv_after > tv_before
        assert abs(tv_after - 0.40 * 1.0 * 100.0) < 0.001

    def test_intrinsic_call_sql(self):
        expr = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        sql = expr.to_sql("data")
        assert "CASE WHEN" in sql
        assert "THEN" in sql
        assert "ELSE" in sql

    def test_intrinsic_call_pure(self):
        expr = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        pure = expr.to_pure("$opt")
        assert "if(" in pure
        assert "$opt.underlying_price" in pure
        assert "$opt.strike" in pure


class TestFXConversion:
    """FX rate conversions and cross-rate computations."""

    def test_usd_to_eur(self):
        fx = FXRate(pair="EUR/USD", rate=1.0850, bid=1.0848, ask=1.0852, amount=1000)
        expected = 1000 / 1.0850
        assert abs(fx.eur_value - expected) < 0.01

    def test_fx_spread_pips(self):
        """FX spread in pips = (ask - bid) * 10000"""
        fx = FXRate(pair="EUR/USD", rate=1.0850, bid=1.0848, ask=1.0852)
        expected = (1.0852 - 1.0848) * 10000
        assert abs(fx.spread_pips - expected) < 0.01

    def test_fx_reacts_to_rate_change(self):
        fx = FXRate(pair="EUR/USD", rate=1.0850, bid=1.0848, ask=1.0852, amount=10000)
        val_before = fx.eur_value

        fx.rate = 1.1000
        val_after = fx.eur_value

        assert val_after < val_before  # EUR strengthened, so fewer EUR per USD


# ===========================================================================
# Risk tests
# ===========================================================================

class TestRiskMetrics:
    """Portfolio risk computations."""

    def test_notional_exposure(self):
        """Notional = quantity * current_price"""
        pos = Position(symbol="AAPL", quantity=500, avg_cost=220.0, current_price=230.0)
        assert pos.notional == 115000.0

    def test_position_weight(self):
        """Weight = notional / portfolio_nav (using fixed 1M NAV)"""
        pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
        expected = (100 * 230.0) / 1_000_000 * 100
        assert abs(pos.weight_pct - expected) < 0.001

    def test_var_proxy(self):
        """Simple parametric VaR proxy = notional * volatility * z_score * sqrt(horizon)"""
        rp = RiskPosition(notional=100_000, daily_vol=0.02, z_score=1.645)

        expected = 100_000 * 0.02 * 1.645
        assert abs(rp.var_1d - expected) < 0.01

        expected_10d = 100_000 * 0.02 * 1.645 * math.sqrt(10)
        assert abs(rp.var_10d - expected_10d) < 0.01

    def test_risk_reacts_to_vol_spike(self):
        rp = RiskPosition(notional=100_000, daily_vol=0.02, z_score=1.645)
        var_before = rp.var_1d

        rp.daily_vol = 0.05  # vol spike
        var_after = rp.var_1d

        assert var_after > var_before
        assert abs(var_after / var_before - 2.5) < 0.01

    def test_stop_loss_alert(self):
        """If unrealized loss > threshold, flag for stop-loss."""
        pos = Position(symbol="TSLA", quantity=100, avg_cost=250.0, current_price=250.0)
        assert pos.stop_loss_status == "OK"

        pos.current_price = 190.0  # -$6000 loss
        assert pos.stop_loss_status == "STOP_LOSS"

    def test_position_limit_breach(self):
        """Alert if notional exceeds limit."""
        pos = Position(symbol="AMZN", quantity=200, avg_cost=180.0, current_price=185.0)
        assert pos.limit_status == "OK"

        pos.current_price = 260.0  # 200 * 260 = 52000
        assert pos.limit_status == "BREACH"


class TestSignalStrength:
    """Signal-based computations using TradingSignal model."""

    def test_weighted_signal(self):
        """Signal score = strength * direction_multiplier"""
        sig = TradingSignal(symbol="AAPL", direction="LONG", strength=0.85, model_name="momentum")
        assert abs(sig.score - 0.85) < 0.001

    def test_short_signal_score(self):
        sig = TradingSignal(symbol="TSLA", direction="SHORT", strength=0.70, model_name="mean_rev")
        assert abs(sig.score - (-0.70)) < 0.001

    def test_signal_confidence_label(self):
        """High/Medium/Low confidence based on strength."""
        sig = TradingSignal(symbol="GOOG", direction="LONG", strength=0.45, model_name="stat_arb")
        assert sig.confidence == "LOW"

        sig.strength = 0.6
        assert sig.confidence == "MEDIUM"

        sig.strength = 0.9
        assert sig.confidence == "HIGH"

    def test_signal_model_label(self):
        """Use string ops on model name."""
        sig = TradingSignal(model_name="momentum_v2", symbol="AAPL", direction="LONG", strength=0.5)
        assert sig.model_upper == "MOMENTUM_V2"
        assert sig.is_momentum is True


# ===========================================================================
# Bond / Fixed Income tests
# ===========================================================================

class TestBondPricing:
    """Simplified bond computations."""

    def test_current_yield(self):
        """Current yield = coupon_rate * face_value / price"""
        bond = Bond(isin="US912828ZT09", face_value=1000, coupon_rate=0.05,
                     price=980.0, yield_to_maturity=0.052, years_to_maturity=10)
        expected = (0.05 * 1000) / 980.0
        assert abs(bond.current_yield - expected) < 0.0001

    def test_annual_coupon(self):
        bond = Bond(face_value=1000, coupon_rate=0.05)
        assert bond.annual_coupon == 50.0

    def test_duration_proxy(self):
        """Simple Macaulay duration proxy = years_to_maturity * (1 - coupon_rate)"""
        bond = Bond(face_value=1000, coupon_rate=0.06, years_to_maturity=5)
        expected = 5 * (1 - 0.06)
        assert abs(bond.duration_proxy - expected) < 0.001

    def test_price_sensitivity(self):
        """Approx price change = -duration * Δyield * price"""
        bond = Bond(face_value=1000, coupon_rate=0.05, price=980.0,
                     yield_to_maturity=0.052, years_to_maturity=7)
        expected_dur = 7 * (1 - 0.05)
        expected_chg = -980.0 * expected_dur * 0.001
        assert abs(bond.price_impact_10bp - expected_chg) < 0.01

    def test_bond_reacts_to_yield_change(self):
        bond = Bond(face_value=1000, coupon_rate=0.04, price=1020.0,
                     yield_to_maturity=0.038, years_to_maturity=5)
        cy_before = bond.current_yield

        bond.price = 950.0  # price drops
        cy_after = bond.current_yield

        assert cy_after > cy_before  # yield rises when price drops

    def test_premium_discount_label(self):
        """Label bond as Premium / Par / Discount."""
        bond = Bond(face_value=1000, price=1020.0)
        assert bond.pricing_label == "PREMIUM"

        bond.price = 980.0
        assert bond.pricing_label == "DISCOUNT"

        bond.price = 1000.0
        assert bond.pricing_label == "PAR"


# ===========================================================================
# Effect / alert tests (trading-specific)
# ===========================================================================

class TestTradingEffects:
    """Test that @effects fire correctly in trading scenarios."""

    def test_pnl_alert_effect(self):
        _pnl_alerts.clear()
        pos = PositionWithPnLEffect(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=220.0)
        initial = len(_pnl_alerts)

        pos.current_price = 230.0
        assert len(_pnl_alerts) > initial
        assert _pnl_alerts[-1] == 1000.0

    def test_multi_field_batch_trade(self):
        """Batch update bid+ask, effect fires once."""
        _mid_ticks.clear()
        md = MarketDataWithMidEffect(symbol="AAPL", bid=100.0, ask=101.0)
        initial = len(_mid_ticks)

        md.batch_update(bid=105.0, ask=106.0)
        batch_fires = len(_mid_ticks) - initial
        assert batch_fires <= 1
        assert _mid_ticks[-1] == 105.5

    def test_multiple_positions_independent(self):
        """Two positions react independently."""
        aapl = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=220.0)
        tsla = Position(symbol="TSLA", quantity=50, avg_cost=250.0, current_price=250.0)

        aapl.current_price = 230.0

        assert aapl.pnl == 1000.0
        assert tsla.pnl == 0.0  # TSLA unchanged


# ===========================================================================
# Serialization roundtrip tests (finance expressions)
# ===========================================================================

class TestFinanceSerialization:
    """Ensure finance expressions survive JSON roundtrip."""

    def test_pnl_expr_roundtrip(self):
        expr = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        restored = from_json(expr.to_json())
        ctx = {"current_price": 230.0, "avg_cost": 220.0, "quantity": 100}
        assert restored.eval(ctx) == 1000.0

    def test_stop_loss_roundtrip(self):
        pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
        expr = If(pnl < Const(-5000), Const("STOP_LOSS"), Const("OK"))
        restored = from_json(expr.to_json())
        assert restored.eval({"current_price": 190.0, "avg_cost": 250.0, "quantity": 100}) == "STOP_LOSS"
        assert restored.eval({"current_price": 248.0, "avg_cost": 250.0, "quantity": 100}) == "OK"

    def test_option_intrinsic_roundtrip(self):
        expr = If(
            Field("underlying_price") > Field("strike"),
            Field("underlying_price") - Field("strike"),
            Const(0),
        )
        restored = from_json(expr.to_json())
        assert restored.eval({"underlying_price": 110.0, "strike": 100.0}) == 10.0
        assert restored.eval({"underlying_price": 90.0, "strike": 100.0}) == 0

    def test_confidence_label_roundtrip(self):
        expr = If(
            Field("strength") > Const(0.75),
            Const("HIGH"),
            If(Field("strength") > Const(0.5), Const("MEDIUM"), Const("LOW")),
        )
        restored = from_json(expr.to_json())
        assert restored.eval({"strength": 0.9}) == "HIGH"
        assert restored.eval({"strength": 0.6}) == "MEDIUM"
        assert restored.eval({"strength": 0.3}) == "LOW"

    def test_bond_yield_roundtrip(self):
        expr = (Field("coupon_rate") * Field("face_value")) / Field("price")
        restored = from_json(expr.to_json())
        ctx = {"coupon_rate": 0.05, "face_value": 1000, "price": 980.0}
        expected = (0.05 * 1000) / 980.0
        assert abs(restored.eval(ctx) - expected) < 0.0001
