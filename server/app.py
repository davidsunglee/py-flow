"""
Deephaven Trading Server
========================
Standalone data engine run by the platform/infra team.
Consumes ticks from the Market Data Server (WebSocket) and publishes
ticking tables to all connected Deephaven clients.

Requires:  Market Data Server running at ws://localhost:8000/md/subscribe
Run:       python3 -i app.py
Web IDE:   http://localhost:10000
"""

import asyncio
import json
import logging
import threading

# ── 1. Start the Deephaven server (must happen before other DH imports) ──────
from deephaven_server import Server

server = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-Dprocess.info.system-info.enabled=false",   # Apple Silicon compat
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
)
server.start()

# ── 2. Deephaven imports (available only after server.start()) ───────────────
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht

# ── 3. Create DynamicTableWriters — the raw ticking data sources ─────────────
price_writer = DynamicTableWriter({
    "Symbol":    dht.string,
    "Price":     dht.double,
    "Bid":       dht.double,
    "Ask":       dht.double,
    "Volume":    dht.int64,
    "Change":    dht.double,
    "ChangePct": dht.double,
})

# Raw append-only table
prices_raw = price_writer.table

# ── 4. Derived tables (published to global scope for clients) ────────────────
# Latest snapshot per symbol — ticks on every update
prices_live = prices_raw.last_by("Symbol")

# Top movers and volume leaders (always available)
top_movers = prices_live.sort_descending("ChangePct")
volume_leaders = prices_live.sort_descending("Volume")

# ── 5. Connect to Market Data Server via WebSocket ───────────────────────────

MD_SERVER_URL = "ws://localhost:8000/md/subscribe"
RECONNECT_DELAY = 2  # seconds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_log = logging.getLogger("dh-md-consumer")


async def _consume_market_data():
    """Connect to the market data server and write ticks to DH writers."""
    import websockets

    while True:
        try:
            _log.info("Connecting to Market Data Server at %s ...", MD_SERVER_URL)
            async with websockets.connect(MD_SERVER_URL) as ws:
                # Subscribe to equity ticks only
                await ws.send(json.dumps({"types": ["equity"]}))
                _log.info("Connected — streaming equity ticks")
                async for msg in ws:
                    tick = json.loads(msg)
                    if tick.get("type") != "equity":
                        continue
                    price_writer.write_row(
                        tick["symbol"],
                        tick["price"],
                        tick["bid"],
                        tick["ask"],
                        tick["volume"],
                        tick["change"],
                        tick["change_pct"],
                    )
        except Exception as e:
            _log.warning(
                "Market Data connection lost (%s). Retrying in %ds...",
                e, RECONNECT_DELAY,
            )
            await asyncio.sleep(RECONNECT_DELAY)


def _start_md_consumer():
    """Run the market data consumer in a background thread with its own loop."""
    asyncio.run(_consume_market_data())


_md_thread = threading.Thread(
    target=_start_md_consumer, daemon=True, name="md-consumer"
)
_md_thread.start()

# ── 6. Print status ─────────────────────────────────────────────────────────
print()
print("=" * 64)
print("  Deephaven Trading Server is RUNNING")
print("  Web IDE:  http://localhost:10000")
print()
print("  Data source: Market Data Server at ws://localhost:8000")
print()
print("  Published tables (available to all clients):")
print("    • prices_raw        — append-only price ticks")
print("    • prices_live       — latest price per symbol")
print("    • top_movers        — symbols ranked by % change")
print("    • volume_leaders    — symbols ranked by volume")
print()
print("  Note: Tables populate once Market Data Server is running.")
print("=" * 64)
print()
