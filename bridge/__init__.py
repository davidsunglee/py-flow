"""
Deephaven ↔ Store Bridge — streams object store events into ticking tables
and pluggable EventSinks.

    StoreBridge   — manages event listener + Deephaven dispatch + sinks
    EventSink     — ABC for pluggable destinations
    LakehouseSink — buffers events, flushes via Lakehouse.ingest()
"""

from bridge.sinks import EventSink
from bridge.sinks.lakehouse import LakehouseSink
from bridge.store_bridge import StoreBridge

__all__ = ["EventSink", "LakehouseSink", "StoreBridge"]
