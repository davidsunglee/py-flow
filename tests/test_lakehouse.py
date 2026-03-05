"""
Lakehouse Tests
================
Unit tests for the lakehouse package: models, table schemas, Arrow conversion,
sync watermarks. Integration tests require Lakekeeper + object store (marked separately).
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
from lakehouse.models import SyncState, TableInfo
from lakehouse.sync import (
    _bars_to_arrow,
    _ensure_tz,
    _tick_rows_to_arrow,
)

# ── Model Tests ─────────────────────────────────────────────────────────────


class TestModels:
    """Tests for SyncState and TableInfo Pydantic models."""

    def test_sync_state_defaults_and_roundtrip(self):
        state = SyncState()
        assert state.events_watermark is None and state.events_synced == 0
        now = datetime.now(timezone.utc)
        state2 = SyncState(events_watermark=now, events_synced=42, last_sync_time=now)
        restored = SyncState(**json.loads(state2.model_dump_json()))
        assert restored.events_synced == 42 and restored.events_watermark is not None

    def test_table_info(self):
        info = TableInfo(name="events")
        assert info.namespace == "default" and info.snapshot_count == 0
        info2 = TableInfo(name="ticks", namespace="prod", current_snapshot_id=12345,
                         snapshot_count=3, schema_fields=["symbol", "price", "timestamp"],
                         partition_fields=["tick_type", "timestamp_day"])
        assert info2.current_snapshot_id == 12345 and len(info2.schema_fields) == 3


# ── Arrow Conversion Tests ──────────────────────────────────────────────────


class TestArrowConversion:
    """Tests for PG/QuestDB row → Arrow table conversion helpers."""

    def test_tick_rows_to_arrow_equity(self):
        now = datetime.now(timezone.utc)
        rows = [
            {"symbol": "AAPL", "price": 150.0, "bid": 149.9, "ask": 150.1,
             "volume": 1000, "change": 1.5, "change_pct": 0.01, "timestamp": now},
            {"symbol": "MSFT", "price": 300.0, "bid": 299.9, "ask": 300.1,
             "volume": 2000, "change": -0.5, "change_pct": -0.002, "timestamp": now},
        ]
        table = _tick_rows_to_arrow("equity", rows)
        assert table.num_rows == 2
        assert table.column("tick_type")[0].as_py() == "equity"
        assert table.column("symbol")[0].as_py() == "AAPL"
        assert table.column("price")[0].as_py() == 150.0

    def test_tick_rows_to_arrow_fx(self):
        now = datetime.now(timezone.utc)
        rows = [
            {"pair": "EUR/USD", "bid": 1.0850, "ask": 1.0852, "mid": 1.0851,
             "spread_pips": 0.2, "currency": "USD", "timestamp": now},
        ]
        table = _tick_rows_to_arrow("fx", rows)
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "EUR/USD"
        assert table.column("mid")[0].as_py() == 1.0851

    def test_tick_rows_to_arrow_curve(self):
        now = datetime.now(timezone.utc)
        rows = [
            {"label": "USD_5Y", "rate": 0.045, "tenor_years": 5.0,
             "discount_factor": 0.82, "currency": "USD", "timestamp": now},
        ]
        table = _tick_rows_to_arrow("curve", rows)
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "USD_5Y"
        assert table.column("rate")[0].as_py() == 0.045

    def test_bars_to_arrow(self):
        now = datetime.now(timezone.utc)
        bars = [
            {"symbol": "AAPL", "open": 150.0, "high": 155.0, "low": 148.0,
             "close": 153.0, "volume": 5000, "trade_count": 100, "timestamp": now},
        ]
        table = _bars_to_arrow(bars, "equity", "1d")
        assert table.num_rows == 1
        assert table.column("tick_type")[0].as_py() == "equity"
        assert table.column("interval")[0].as_py() == "1d"
        assert table.column("high")[0].as_py() == 155.0

    def test_bars_to_arrow_with_bar_objects(self):
        """Test conversion with objects that have attributes instead of dict keys."""
        now = datetime.now(timezone.utc)

        class MockBar:
            def __init__(self):
                self.symbol = "MSFT"
                self.open = 300.0
                self.high = 310.0
                self.low = 295.0
                self.close = 305.0
                self.volume = 3000
                self.trade_count = 50
                self.timestamp = now

        bars = [MockBar()]
        table = _bars_to_arrow(bars, "equity", "1d")
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "MSFT"


# ── Timezone Helper Tests ───────────────────────────────────────────────────


class TestEnsureTz:
    """Tests for the _ensure_tz helper."""

    def test_ensure_tz_cases(self):
        assert _ensure_tz(None) is None
        assert _ensure_tz("not a datetime") is None  # type: ignore[arg-type]
        naive = datetime(2025, 1, 1, 12, 0, 0)
        assert _ensure_tz(naive).tzinfo == timezone.utc  # type: ignore[union-attr]
        aware = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert _ensure_tz(aware) is aware


# ── Catalog Tests ───────────────────────────────────────────────────────────


class TestCatalogConfig:
    """Tests for catalog configuration logic."""

    def test_default_env_vars(self):
        from lakehouse.catalog import (
            DEFAULT_CATALOG_URI,
            DEFAULT_S3_ACCESS_KEY,
            DEFAULT_S3_ENDPOINT,
            DEFAULT_WAREHOUSE,
        )
        assert "8181" in DEFAULT_CATALOG_URI
        assert "9002" in DEFAULT_S3_ENDPOINT
        assert DEFAULT_WAREHOUSE == "lakehouse"
        assert DEFAULT_S3_ACCESS_KEY == "minioadmin"


# ── Service Manager Tests ──────────────────────────────────────────────────


class TestServiceManagers:
    """Tests for LakekeeperManager and MinIO backend configuration."""

    def test_lakekeeper_defaults_and_custom(self):
        from lakehouse.services import LakekeeperManager
        mgr = LakekeeperManager()
        assert mgr._port == 8181 and mgr.catalog_url == "http://localhost:8181/catalog"
        mgr2 = LakekeeperManager(port=9999)
        assert mgr2.catalog_url == "http://localhost:9999/catalog"

    def test_minio_defaults_and_custom(self):
        from objectstore._minio import _MinIOBackend
        mgr = _MinIOBackend()
        assert mgr._api_port == 9002 and mgr.endpoint == "http://localhost:9002"
        mgr2 = _MinIOBackend(api_port=9010, console_port=9011)
        assert mgr2.endpoint == "http://localhost:9010"


# ── Sync Engine Tests ──────────────────────────────────────────────────────


class TestSyncEngine:
    """Tests for SyncEngine watermark and state logic."""

    def test_state_persistence(self, tmp_path):
        """Test that sync state round-trips through JSON."""
        state_file = tmp_path / "sync_state.json"

        mock_lakehouse = MagicMock()
        from lakehouse.sync import SyncEngine
        engine = SyncEngine(lakehouse=mock_lakehouse, state_path=str(state_file))

        assert engine.state.events_synced == 0

        # Manually update state
        engine._state.events_synced = 100
        engine._state.events_watermark = datetime(2025, 6, 1, tzinfo=timezone.utc)
        engine._save_state()

        # Create new engine reading same file
        engine2 = SyncEngine(lakehouse=mock_lakehouse, state_path=str(state_file))
        assert engine2.state.events_synced == 100
        assert engine2.state.events_watermark is not None

    def test_sync_all_with_no_sources(self, tmp_path):
        """Test sync_all with no sources provided."""
        state_file = tmp_path / "sync_state.json"
        mock_lakehouse = MagicMock()

        from lakehouse.sync import SyncEngine
        engine = SyncEngine(lakehouse=mock_lakehouse, state_path=str(state_file))

        result = engine.sync_all()
        assert result == {"ticks": 0, "bars": 0}


# ── Platform Detection Tests ───────────────────────────────────────────────


class TestPlatformDetection:
    """Tests for binary download URL detection."""

    def test_download_urls(self):
        from lakehouse.services import _lakekeeper_archive_name
        from objectstore._minio import _minio_download_url
        name = _lakekeeper_archive_name()
        assert name.endswith(".tar.gz") and "lakekeeper" in name
        assert "dl.min.io" in _minio_download_url()
