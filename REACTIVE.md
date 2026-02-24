# @computed + @effect: Pure OO Reactive Properties

Three user-facing decorators — `@computed`, `@effect`, and `batch_update()` — give full reactivity through plain classes and methods. No `ReactiveGraph`, no `Signal`, no `Computed`, no `Effect` — zero framework internals exposed. reaktiv is a hidden implementation detail.

## User-facing API

```python
@dataclass
class Position(Storable):
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0

    @computed
    def pnl(self):
        return (self.current_price - self.avg_cost) * self.quantity

    @computed
    def market_value(self):
        return self.current_price * self.quantity

    @computed
    def alert(self):
        if self.pnl < -5000:
            return "STOP_LOSS"
        return "OK"

    @effect("pnl")
    def check_stop_loss(self, value):
        if value < -5000:
            send_alert(f"Stop loss on {self.symbol}: ${value}")

    @effect("market_value")
    def log_mv(self, value):
        logger.info(f"{self.symbol} MV: ${value:,.0f}")

# Single object — fully reactive
pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
print(pos.pnl)              # 1000.0
pos.current_price = 235.0   # triggers cascade → pnl recomputes → effects fire
print(pos.pnl)              # 1500.0

# Cross-entity — just another class
@dataclass
class Portfolio(Storable):
    positions: list = field(default_factory=list)

    @computed
    def total_mv(self):
        return sum(p.market_value for p in self.positions)

    @computed
    def total_pnl(self):
        return sum(p.pnl for p in self.positions)

    @effect("total_pnl")
    def risk_check(self, value):
        if value < -50000:
            send_alert("Portfolio stop loss!")

book = Portfolio(positions=[pos1, pos2, pos3])
print(book.total_mv)        # sum of all positions' market_value
pos1.current_price = 240.0  # book.total_mv auto-recomputes → risk_check may fire

# Batch update — method on Storable, no framework import
pos.batch_update(current_price=240.0, quantity=150)  # single recomputation

# SQL/Pure compilation still works (single-entity @computed only)
Position.pnl.expr.to_sql("data")
Position.pnl.expr.to_pure("$pos")
```

**That's it.** `@computed` for derived values, `@effect` for side effects, `batch_update()` for multi-field changes.

---

## How cross-entity works

reaktiv records every Signal/Computed read during a Computed's evaluation. Those become its dependencies.

```
Portfolio.__post_init__ creates:
  _signals["positions"] = Signal([pos1, pos2, pos3])
  _computeds["total_mv"] = Computed(fn)

When total_mv evaluates, fn() executes through a reactive proxy:
  1. proxy.positions → _signals["positions"]()   ← reaktiv records dependency
     → returns [pos1, pos2, pos3]
  2. p.market_value → p._computeds["market_value"]()  ← recorded
     → which reads p._signals["current_price"]()       ← recorded
     → and p._signals["quantity"]()                     ← recorded
  3. sum() → returns total

reaktiv now knows the full dependency chain:
  total_mv → [positions_signal, pos1.mv, pos2.mv, pos3.mv]
  pos1.mv  → [pos1.current_price, pos1.quantity]
```

When `pos1.current_price = 240.0`:
1. `__setattr__` → `pos1._signals["current_price"].set(240.0)`
2. reaktiv: pos1.market_value depends on this → dirty
3. reaktiv: portfolio.total_mv depends on pos1.market_value → dirty
4. Next read of `portfolio.total_mv` → recomputes entire chain

---

## Side-effect strategy

| Need | Mechanism |
|------|-----------|
| **Lightweight** (log, alert, push) | `@effect("computed_name")` on the class |
| **Durable** (send email, call API) | WorkflowEngine |
| **Store events** (on write/update) | EventBus / PG LISTEN/NOTIFY |
| **State transitions** | State machine hooks (3-tier) |

`@effect` is fire-and-forget — exceptions logged but swallowed (same as Tier 2 state machine hooks).

---

## Under the hood

### Decoration time
`@computed` calls `inspect.getsource()` + `ast.parse()` → builds Expr tree. Stored on descriptor as `.expr`.

- **Single-entity** `@computed` (arithmetic, comparisons, conditionals): full Expr tree → compiles to Python / SQL / Legend Pure.
- **Cross-entity** `@computed` (iteration over lists, aggregation): no Expr tree. Uses a proxy-based runtime evaluation that reads from Signals/Computeds for reactivity.

### Instantiation (`__post_init__`)
1. `Signal` for each dataclass field → `_signals` dict
2. `Computed` for each `@computed` → `_computeds` dict
3. `Effect` for each `@effect` → `_effects` dict
4. Single-entity: Computed reads from field Signals via Expr eval
5. Cross-entity: Computed calls original function with a proxy `self` that reads from Signals

### `__setattr__`
`pos.current_price = 235.0` → updates attribute AND calls `_signals["current_price"].set(235.0)` → cascade.

### Proxy for cross-entity
```python
# @computed def total_mv(self): return sum(p.market_value for p in self.positions)
# At runtime, Computed calls fn(proxy) where proxy.__getattr__:
#   "positions" → self._signals["positions"]()  → gets list (tracked)
#   p.market_value → goes through ComputedProperty descriptor → p._computeds["market_value"]() (tracked)
```

### Python → Expr mapping (single-entity)

| Python syntax | Expr node |
|---------------|-----------|
| `self.x` (field) | `Field("x")` |
| `self.y` (@computed) | inlines y's Expr |
| `42`, `"hello"` | `Const(...)` |
| `a + b`, `a * b`, etc. | `BinOp(...)` |
| `a > b`, `a == b` | `BinOp(...)` |
| `a and b`, `not x` | `BinOp("and",...)`/`UnaryOp("not",...)` |
| `-x`, `abs(x)` | `UnaryOp(...)` |
| `x if c else y` | `If(c, x, y)` |
| `if c: return x; return y` | `If(c, x, y)` |
| `math.sqrt(x)`, `round(x)` | `Func("sqrt",[x])` |
| `min(a,b)`, `max(a,b)` | `Func("min",[a,b])` |

### Cross-entity @computed (proxy-based, Python-only)

| Pattern | How it works |
|---------|-------------|
| `sum(p.x for p in self.items)` | `self.items` reads Signal; `p.x` reads Computed descriptor |
| `len(self.positions)` | `self.positions` reads Signal |
| `self.other_computed` | reads from `_computeds` |

### Unsupported Python (raises `ComputedParseError`)

Assignments, try/except, imports, yield, await, class/function defs, global/nonlocal.

---

## Key design decisions

- **Zero framework leakage** — users see `@computed`, `@effect`, `batch_update()`, classes, attribute assignment. Nothing else.
- **Always reactive** — @computed = Signal from birth. No opt-in.
- **Two flavors, one decorator** — single-entity gets Expr (SQL/Pure); cross-entity gets proxy (Python-only). Transparent to user.
- **@effect = fire-and-forget** — exceptions logged but swallowed (same as Tier 2 state machine hooks).
- **Cross-entity = just a class** — Portfolio is a Storable with a list field and @computed methods.
- **Serialization** — `to_json()`/`from_json()` skip reactive internals. `__post_init__` re-creates them.
- **Expr tree unchanged** — still used for guards, filters, SQL push-down. @computed just generates them from Python syntax.
