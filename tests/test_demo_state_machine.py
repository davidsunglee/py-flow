"""
Mirror test for demo_state_machine.py
=======================================
Verifies the full demo flow — three-tier state machine side-effects:

  Tier 1 (action):          Atomic with DB — rolls back if it fails.
  Tier 2 (on_enter/on_exit): Fire-and-forget after commit.
  Tier 3 (start_workflow):  Durable workflow dispatch after commit.
"""

from dataclasses import dataclass
from typing import ClassVar

import pytest

from store import StateMachine, Storable, Transition, connect
from workflow import create_engine


# ── Tier 3 workflow ──────────────────────────────────────────────────────

_settlement_log: list[str] = []


def settlement_workflow(entity_id):
    """Durable workflow: runs to completion even if process restarts."""
    _settlement_log.append(f"settlement:{entity_id}")


# ── Tier 1 + 2 callbacks ────────────────────────────────────────────────

_action_log: list[str] = []
_hook_log: list[str] = []


def _book_settlement(obj, from_state, to_state):
    _action_log.append(f"action:{from_state}->{to_state}")


def _log_exit(obj, from_state, to_state):
    _hook_log.append(f"exit:{from_state}")


def _log_enter(obj, from_state, to_state):
    _hook_log.append(f"enter:{to_state}")


class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions: ClassVar[list] = [
        Transition("PENDING", "FILLED",
                   guard=lambda obj: obj.quantity > 0,
                   action=_book_settlement,
                   on_exit=_log_exit,
                   on_enter=_log_enter,
                   start_workflow=settlement_workflow),
        Transition("PENDING", "CANCELLED",
                   on_exit=_log_exit,
                   on_enter=_log_enter,
                   allowed_by=["risk_manager"]),
        Transition("FILLED", "SETTLED",
                   action=lambda obj, f, t: _action_log.append("action:FILLED->SETTLED"),
                   on_enter=lambda obj, f, t: _hook_log.append("enter:SETTLED")),
    ]


@dataclass
class SMOrder(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""


SMOrder._state_machine = OrderLifecycle


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def engine(store_server, workflow_server):
    """Wire workflow engine to the store — same as demo."""
    store_server.provision_user("sm_trader", "sm_pw")
    store_server.register_alias("sm-demo")
    workflow_server.register_alias("sm-demo")
    eng = create_engine("sm-demo", name="sm-tiers")
    eng.launch()
    SMOrder._workflow_engine = eng
    yield eng
    eng.destroy()


@pytest.fixture(scope="module")
def db(engine):
    """User connection for state machine tests."""
    conn = connect("sm-demo", user="sm_trader", password="sm_pw")
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def _clear_logs():
    """Reset logs before each test."""
    _settlement_log.clear()
    _action_log.clear()
    _hook_log.clear()


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoStateMachine:
    """Mirrors demo_state_machine.py — three-tier side effects."""

    def test_all_three_tiers_on_fill(self, db) -> None:
        """Demo 1: PENDING → FILLED fires all three tiers."""
        order = SMOrder(symbol="AAPL", quantity=100, price=228.50, side="BUY")
        order.save()
        assert order.state == "PENDING"

        order.transition("FILLED")
        assert order.state == "FILLED"

        # Tier 1: action fired
        assert any("PENDING->FILLED" in a for a in _action_log)
        # Tier 2: hooks fired
        assert any("exit:PENDING" in h for h in _hook_log)
        assert any("enter:FILLED" in h for h in _hook_log)
        # Tier 3: workflow dispatched asynchronously via DBOS
        import time
        for _ in range(20):
            if _settlement_log:
                break
            time.sleep(0.1)
        assert len(_settlement_log) >= 1
        assert order.entity_id in _settlement_log[0]

    def test_tier1_rollback_on_failure(self, db) -> None:
        """Demo 2: Action failure → state rolls back to original."""

        class FailLifecycle(StateMachine):
            initial = "NEW"
            transitions: ClassVar[list] = [
                Transition("NEW", "DONE",
                           action=lambda obj, f, t: (_ for _ in ()).throw(
                               ValueError("Settlement system unavailable!"))),
            ]

        order = SMOrder(symbol="MSFT", quantity=50, price=415.0, side="SELL")
        order._state_machine = FailLifecycle  # type: ignore[misc]
        order.save()
        assert order.state == "NEW"

        with pytest.raises(ValueError, match="Settlement system unavailable"):
            order.transition("DONE")

        # State rolled back — reload from DB to prove it
        fresh = SMOrder.find(order.entity_id)
        assert fresh is not None
        assert fresh.state == "NEW"

    def test_tier2_failure_swallowed(self, db) -> None:
        """Demo 3: Hook failure → state still committed (fire-and-forget)."""

        class FragileLifecycle(StateMachine):
            initial = "ALPHA"
            transitions: ClassVar[list] = [
                Transition("ALPHA", "BETA",
                           on_enter=lambda obj, f, t: (_ for _ in ()).throw(
                               RuntimeError("Notification service down!"))),
            ]

        order = SMOrder(symbol="GOOG", quantity=25, price=175.0, side="BUY")
        order._state_machine = FragileLifecycle  # type: ignore[misc]
        order.save()
        assert order.state == "ALPHA"

        order.transition("BETA")
        assert order.state == "BETA"  # committed despite hook failure

    def test_guard_blocks_transition(self, db) -> None:
        """Guard: quantity > 0 required for PENDING → FILLED."""
        order = SMOrder(symbol="TSLA", quantity=0, price=355.0, side="BUY")
        order.save()
        assert order.state == "PENDING"

        # Guard should block transition (quantity == 0)
        with pytest.raises(Exception):
            order.transition("FILLED")

        assert order.state == "PENDING"

    def test_second_transition_filled_to_settled(self, db) -> None:
        """FILLED → SETTLED fires its own action + hook."""
        order = SMOrder(symbol="AMZN", quantity=10, price=225.0, side="BUY")
        order.save()
        order.transition("FILLED")
        _action_log.clear()
        _hook_log.clear()

        order.transition("SETTLED")
        assert order.state == "SETTLED"
        assert any("FILLED->SETTLED" in a for a in _action_log)
        assert any("enter:SETTLED" in h for h in _hook_log)
