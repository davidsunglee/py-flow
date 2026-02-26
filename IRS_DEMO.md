# IRS Demo — Fully Reactive Grid via Market Data Hub

The IRS demo consumes live FX ticks from the Market Data Server and prices an entire swap book using **only** `@computed` and `@effect` — no imperative push helpers, no imperative derivation functions. One `batch_update()` call triggers the full cascade from FX through curves to portfolio NPV, including all Deephaven table writes.

## Architecture

```
┌─────────────────────────────┐       ┌──────────────────────────────────┐
│  Market Data Server (:8000) │       │  IRS Demo (Deephaven :10000)     │
│                             │  WS   │                                  │
│  SimulatorFeed:             │◄─────►│  Consumes FX ticks               │
│    • 8 equity ticks (GBM)   │       │  @computed derives curves from FX│
│    • 3 FX ticks (random     │       │  @computed derives swap float_rate│
│      walk: USD/JPY,         │       │  @effect pushes to DH + publishes│
│      EUR/USD, GBP/USD)      │       │    CurveTicks back to hub        │
│                             │       │                                  │
│  TickBus (bidirectional)    │       │  One batch_update() triggers ALL │
│    • Clients publish back   │       │                                  │
└─────────────────────────────┘       └──────────────────────────────────┘
```

## Running

```bash
# Terminal 1: Start Market Data Server
python -m marketdata.server

# Terminal 2: Start IRS Demo
python3 demo_irs.py

# Open http://localhost:10000 in your browser
```

## Reactive Chain

A single call in the WS consumer loop:

```python
fx.batch_update(bid=tick["bid"], ask=tick["ask"])
```

Triggers this **entire** chain — every step is `@computed` or `@effect`, no imperative code:

```
FXSpot.batch_update(bid, ask)
  │
  ├→ @computed mid                          (single-entity)
  │    └→ @effect on_mid                    → self.dh_write()
  │
  ├→ @computed YieldCurvePoint.rate         (cross-entity: reads fx_ref.mid)
  │    │  rate = base_rate + sensitivity * (fx.mid - fx_base) / fx_base
  │    ├→ @computed discount_factor
  │    └→ @effect on_rate                   → self.dh_write()
  │                                         → queue CurveTick for publish-back
  │
  ├→ @computed InterestRateSwap.float_rate  (cross-entity: reads curve_ref.rate)
  │    ├→ @computed npv, dv01, pnl_status
  │    └→ @effect on_npv                    → self.dh_write()
  │
  └→ @computed SwapPortfolio.total_npv      (cross-entity: reads swaps[].npv)
       └→ @effect on_total_npv              → self.dh_write()
```

After `batch_update()` returns, the WS loop just:
1. Flushes the DH update graph (`requestRefresh()`)
2. Drains the CurveTick publish queue back to the hub

**No `push_fx()`, `push_curve()`, `push_swaps()`, `push_portfolio()`.** Every DH write is an `@effect` calling `self.dh_write()` — the `@dh_table` decorator auto-creates the writer and tables from the class definition.

## Cross-Entity Wiring

The key innovation is **structural references** passed at construction time:

```python
# FX spots created first
fx_spots = {"EUR/USD": FXSpot(...), "USD/JPY": FXSpot(...), ...}

# Curve points reference their benchmark FX pair
YieldCurvePoint(..., fx_ref=fx_spots["EUR/USD"], fx_base_mid=fx_spots["EUR/USD"].mid)

# Swaps reference their tenor-matched curve point
InterestRateSwap(..., curve_ref=usd_curve_points["USD_5Y"])

# Portfolios reference their child swaps
SwapPortfolio(name="ALL", swaps=list(swaps.values()))
```

The reactive framework (`reaktiv`) tracks dependencies automatically through these references — when `FXSpot.mid` changes, `YieldCurvePoint.rate` recomputes because its `@computed` body reads `self.fx_ref.mid`.

## Deephaven Tables

Open `http://localhost:10000` to see all ticking tables:

| Table | Description |
|---|---|
| `fx_spot_live` | FX spot rates (from market data server) |
| `yield_curve_point_live` | Yield curve points (`@computed` from FX) |
| `interest_rate_swap_live` | IRS pricing: NPV, DV01, PnL status (`@computed` cascade) |
| `swap_summary` | Aggregate NPV + DV01 |
| `swap_portfolio_live` | Portfolio breakdown: ALL / USD / JPY (`@computed`) |

Raw append-only tables: `fx_spot_raw`, `yield_curve_point_raw`, `interest_rate_swap_raw`, `swap_portfolio_raw`

Table names are auto-derived from class names via `@dh_table` (CamelCase → snake_case).

## What Makes This Interesting

- **Purely reactive** — no imperative derivation or push functions; `@computed` for calculations, `@effect` for side-effects only
- **Cross-entity dependencies** — curve rates derive from FX spots, swap float_rates derive from curve rates, portfolio totals derive from swap NPVs
- **One call does it all** — `batch_update(bid, ask)` cascades through the entire graph including DH writes
- **Bidirectional** — the demo both consumes (FX) and publishes (curves) through the same hub
- **No internal simulation** — all market data comes from the external server; rate derivation is deterministic from FX moves
- **Initial state is automatic** — `@effects` fire during construction, so DH tables are populated with no explicit push calls

## Domain Models

### FXSpot
```python
@dh_table
class FXSpot(Storable):
    __key__ = "pair"
    pair: str; bid: float; ask: float; currency: str

    @computed mid          # (bid + ask) / 2
    @computed spread_pips  # (ask - bid) * 10000
    @effect("mid") on_mid  # → self.dh_write()
```

### YieldCurvePoint
```python
@dh_table(exclude={"base_rate", "sensitivity", "fx_base_mid"})
class YieldCurvePoint(Storable):
    __key__ = "label"
    label: str; tenor_years: float; base_rate: float
    sensitivity: float; currency: str
    fx_ref: FXSpot; fx_base_mid: float  # cross-entity ref

    @computed rate             # base_rate + sensitivity * fx_pct_move
    @computed discount_factor  # 1 / (1 + rate) ^ tenor
    @effect("rate") on_rate    # → self.dh_write() + CurveTick queue
```

### InterestRateSwap
```python
@dh_table
class InterestRateSwap(Storable):
    __key__ = "symbol"
    symbol: str; notional: float; fixed_rate: float
    tenor_years: float; currency: str
    curve_ref: YieldCurvePoint  # cross-entity ref (auto-skipped: object)

    @computed float_rate    # curve_ref.rate (auto-tracks dependency)
    @computed fixed_leg_pv; @computed float_leg_pv
    @computed npv; @computed dv01; @computed pnl_status -> str
    @effect("npv") on_npv   # → self.dh_write()
```

### SwapPortfolio
```python
@dh_table
class SwapPortfolio(Storable):
    __key__ = "name"
    name: str; swaps: list  # cross-entity refs (auto-skipped: list)

    @computed total_npv; @computed total_dv01
    @computed max_npv; @computed min_npv; @computed swap_count -> int
    @effect("total_npv") on_total_npv  # → self.dh_write()
```

## Multi-Process Potential

In this demo, one process does it all. But the hub architecture supports splitting:

- **Process A** (market maker): publishes FX ticks
- **Process B** (curve builder): subscribes FX → derives curves → publishes CurveTicks
- **Process C** (swap pricer): subscribes curves → prices swaps → publishes risk

## Market Data Server API

| Endpoint | Description |
|---|---|
| `GET /md/health` | Health check with per-type asset counts |
| `GET /md/symbols` | Symbol universe grouped by type |
| `GET /md/snapshot` | All latest messages |
| `GET /md/snapshot/{type}` | Latest by type (equity/fx/curve) |
| `GET /md/snapshot/{type}/{symbol}` | Single item |
| `POST /md/publish` | Publish any MarketDataMessage to the bus |
| `WS /md/subscribe` | Bidirectional: subscribe with type+symbol filters, publish messages back |
