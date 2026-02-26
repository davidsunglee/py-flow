"""
@dh_table — class decorator that auto-creates Deephaven DynamicTableWriter,
raw (append-only) and live (last_by) tables from a Storable dataclass.

Usage:
    @dh_table
    @dataclass
    class FXSpot(Storable):
        __key__ = "pair"
        pair: str = ""
        bid: float = 0.0
        ...

    @dh_table(exclude={"base_rate", "sensitivity", "fx_base_mid"})
    @dataclass
    class YieldCurvePoint(Storable):
        __key__ = "label"
        ...

Adds to the class:
    cls._dh_writer      DynamicTableWriter instance
    cls._dh_raw         raw (append-only) table
    cls._dh_live        live (last_by __key__) table
    cls._dh_columns     [(dh_col_name, attr_name, dh_type), ...]
    cls._dh_table_name  snake_case name derived from class name
    self.dh_write()     instance method — writes all column values to the writer
"""

import re

# Global registry: table_name → (writer, raw, live)
_registry = {}


def _to_snake_case(name):
    """Convert CamelCase class name to snake_case table name.

    FXSpot           → fx_spot
    YieldCurvePoint  → yield_curve_point
    InterestRateSwap → interest_rate_swap
    SwapPortfolio    → swap_portfolio
    """
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


# Primitive types that map to DH columns
_PRIMITIVE_TYPES = {str, float, int, bool}


def _resolve_column_specs(cls, exclude=None):
    """Pure-Python column resolution — no DH imports needed.

    Returns list of (col_name, attr_name, python_type).
    Skips non-primitive fields (object, list, etc.) and anything in exclude.
    """
    from reactive.computed import ComputedProperty

    exclude = set(exclude) if exclude else set()
    specs = []

    # 1. Dataclass fields (in definition order)
    for fname, fobj in cls.__dataclass_fields__.items():
        if fname in exclude or fname.startswith("_"):
            continue
        py_type = fobj.type
        if isinstance(py_type, str):
            py_type = {"str": str, "float": float, "int": int, "bool": bool}.get(py_type)
        if py_type not in _PRIMITIVE_TYPES:
            continue  # skip object, list, etc.
        specs.append((fname, fname, py_type))

    # 2. @computed properties (sorted for deterministic order)
    computed_names = sorted(
        name
        for name in dir(cls)
        if not name.startswith("_")
        and name not in exclude
        and isinstance(getattr(cls, name, None), ComputedProperty)
    )
    for name in computed_names:
        cp = getattr(cls, name)
        ret = getattr(cp.fn, "__annotations__", {}).get("return", float)
        if ret not in _PRIMITIVE_TYPES:
            ret = float  # default to float for unannotated computed
        specs.append((name, name, ret))

    return specs


def _resolve_columns(cls, exclude):
    """Build DH column specs: maps Python types to deephaven dtypes.

    Returns list of (col_name, attr_name, dh_type).
    """
    from deephaven import dtypes as dht

    type_map = {
        str: dht.string,
        float: dht.double,
        int: dht.int32,
        bool: dht.bool_,
    }

    return [
        (col, attr, type_map[py_type])
        for col, attr, py_type in _resolve_column_specs(cls, exclude)
    ]


def _dh_write(self):
    """Write all DH column values to the writer. Added to decorated classes."""
    cls = type(self)
    cls._dh_writer.write_row(*(getattr(self, attr) for _, attr, _ in cls._dh_columns))


def _apply_dh_table(cls, exclude=None):
    """Core logic: create writer, tables, attach to class."""
    from deephaven import DynamicTableWriter

    # Require __key__
    key = getattr(cls, "__key__", None)
    if key is None:
        raise ValueError(
            f"@dh_table on {cls.__name__} requires a __key__ class variable "
            f"(e.g. __key__ = 'symbol')"
        )

    # Resolve columns
    col_specs = _resolve_columns(cls, exclude)
    if not col_specs:
        raise ValueError(f"@dh_table on {cls.__name__}: no columns resolved")

    # Table name from class name
    table_name = _to_snake_case(cls.__name__)

    # Create writer
    schema = {dh_name: dh_type for dh_name, _, dh_type in col_specs}
    writer = DynamicTableWriter(schema)

    # Create tables
    raw = writer.table
    live = raw.last_by(key)

    # Attach to class
    cls._dh_writer = writer
    cls._dh_raw = raw
    cls._dh_live = live
    cls._dh_columns = col_specs
    cls._dh_table_name = table_name
    cls.dh_write = _dh_write

    # Register
    _registry[table_name] = (writer, raw, live)

    return cls


def dh_table(cls=None, *, exclude=None):
    """Class decorator: auto-create DH writer + tables from Storable fields.

    Supports both bare and parameterized usage:
        @dh_table                          # auto-infer all columns
        @dh_table(exclude={"internal"})    # skip specific fields
    """
    if cls is not None:
        # Bare @dh_table (no parentheses)
        return _apply_dh_table(cls)
    # Parameterized @dh_table(exclude=...)
    def decorator(cls):
        return _apply_dh_table(cls, exclude=exclude)
    return decorator


def get_dh_tables():
    """Return dict of all registered tables: {name_raw: table, name_live: table}."""
    tables = {}
    for name, (_writer, raw, live) in _registry.items():
        tables[f"{name}_raw"] = raw
        tables[f"{name}_live"] = live
    return tables
