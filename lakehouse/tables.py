"""
Lakehouse Table Definitions (DEPRECATED)
=========================================
All table creation is now handled by ``Lakehouse.ingest()`` which
auto-creates Iceberg tables as needed. This module is kept as a
namespace placeholder.

The old pre-defined schemas (EVENTS_SCHEMA, TICKS_SCHEMA, etc.) and
``ensure_tables()`` have been removed. Tables are created on first
write via ``Lakehouse.ingest()``.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
