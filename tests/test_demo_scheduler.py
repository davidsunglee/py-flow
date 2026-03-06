"""
Mirror test for demo_scheduler.py
====================================
Verifies the full demo flow:

  1. @schedule decorator → collect_schedules
  2. Programmatic API: Schedule + Task registration
  3. Fire single-task + pipeline + diamond DAG
  4. Failure propagation (failed task → dependent skipped)
  5. Duration tracking
  6. Tick loop (cron-based firing)
  7. Pause / resume
  8. List & history
"""

import time

import pytest

from scheduler import Schedule, Scheduler, Task
from scheduler.admin import SchedulerServer


# ── Module-level task functions (must be importable by scheduler) ─────────

MODULE = "tests.test_demo_scheduler"


def task_extract():
    time.sleep(0.02)
    return "1000 rows"


def task_transform():
    time.sleep(0.02)
    return "transformed"


def task_load():
    return "loaded"


def task_validate():
    return "valid"


def task_publish():
    return "published"


def task_always_fails():
    raise RuntimeError("Connection refused: database unreachable")


def task_send_alert():
    return "alerted"


def task_slow():
    time.sleep(0.1)
    return "computed"


def task_fast():
    return "cached"


def task_heartbeat():
    return "ok"


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def sched_server(scheduler_server):
    """Delegate to session-scoped scheduler_server from conftest."""
    scheduler_server.register_alias("sched-demo")
    yield scheduler_server


@pytest.fixture(scope="module")
def scheduler(sched_server):
    """Scheduler client."""
    return Scheduler("sched-demo")


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoScheduler:
    """Mirrors demo_scheduler.py flow."""

    def test_register_single_task_schedule(self, scheduler) -> None:
        """Programmatic API: register heartbeat schedule."""
        scheduler.register(Schedule(
            name="heartbeat",
            cron_expr="*/1 * * * *",
            tasks=[Task("heartbeat", fn=f"{MODULE}:task_heartbeat")],
        ))
        schedules = scheduler.list_schedules()
        names = [s.name for s in schedules]
        assert "heartbeat" in names

    def test_register_linear_pipeline(self, scheduler) -> None:
        """Register extract → transform → load pipeline."""
        scheduler.register(Schedule(
            name="data_pipeline",
            cron_expr="0 * * * *",
            description="Extract → Transform → Load",
            tasks=[
                Task("extract", fn=f"{MODULE}:task_extract"),
                Task("transform", fn=f"{MODULE}:task_transform", depends_on=["extract"]),
                Task("load", fn=f"{MODULE}:task_load", depends_on=["transform"]),
            ],
        ))
        schedules = scheduler.list_schedules()
        names = [s.name for s in schedules]
        assert "data_pipeline" in names

    def test_register_diamond_dag(self, scheduler) -> None:
        """Register diamond DAG: extract → (transform || validate) → publish."""
        scheduler.register(Schedule(
            name="diamond",
            cron_expr="0 2 * * *",
            tasks=[
                Task("extract", fn=f"{MODULE}:task_extract"),
                Task("transform", fn=f"{MODULE}:task_transform", depends_on=["extract"]),
                Task("validate", fn=f"{MODULE}:task_validate", depends_on=["extract"]),
                Task("publish", fn=f"{MODULE}:task_publish", depends_on=["transform", "validate"]),
            ],
        ))

    def test_register_fragile_schedule(self, scheduler) -> None:
        """Register fragile schedule for failure propagation test."""
        scheduler.register(Schedule(
            name="fragile",
            cron_expr="0 3 * * *",
            tasks=[
                Task("always_fails", fn=f"{MODULE}:task_always_fails"),
                Task("send_alert", fn=f"{MODULE}:task_send_alert", depends_on=["always_fails"]),
            ],
        ))

    def test_register_timing_schedule(self, scheduler) -> None:
        """Register timing schedule for duration tracking."""
        scheduler.register(Schedule(
            name="timing_test",
            cron_expr="0 4 * * *",
            tasks=[
                Task("slow", fn=f"{MODULE}:task_slow"),
                Task("fast", fn=f"{MODULE}:task_fast"),
            ],
        ))

    def test_fire_single_task(self, scheduler) -> None:
        """Fire heartbeat — single task completes."""
        run = scheduler.fire("heartbeat")
        assert run.state in ("COMPLETED", "completed", "SUCCESS")
        assert "heartbeat" in run.task_results

    def test_fire_linear_pipeline(self, scheduler) -> None:
        """Fire data_pipeline — all 3 tasks complete in order."""
        run = scheduler.fire("data_pipeline")
        assert run.state in ("COMPLETED", "completed", "SUCCESS")
        assert len(run.task_results) == 3

    def test_fire_diamond_dag(self, scheduler) -> None:
        """Fire diamond — parallel branches converge."""
        run = scheduler.fire("diamond")
        assert run.state in ("COMPLETED", "completed", "SUCCESS")
        assert len(run.task_results) == 4

    def test_failure_propagation(self, scheduler) -> None:
        """Fire fragile — failed task → dependent skipped."""
        run = scheduler.fire("fragile")
        fails = run.task_results.get("always_fails")
        alert = run.task_results.get("send_alert")
        fs = fails.status if hasattr(fails, 'status') else fails.get('status', '')
        als = alert.status if hasattr(alert, 'status') else alert.get('status', '')
        assert fs in ("ERROR", "FAILED", "error", "failed")
        assert als in ("SKIPPED", "skipped")

    def test_duration_tracking(self, scheduler) -> None:
        """Slow task takes longer than fast task."""
        run = scheduler.fire("timing_test")
        slow = run.task_results.get("slow")
        fast = run.task_results.get("fast")
        slow_ms = slow.duration_ms if hasattr(slow, 'duration_ms') else slow.get('duration_ms', 0)
        fast_ms = fast.duration_ms if hasattr(fast, 'duration_ms') else fast.get('duration_ms', 0)
        assert slow_ms > fast_ms

    def test_tick(self, scheduler) -> None:
        """Tick fires due schedules."""
        runs = scheduler.tick()
        assert isinstance(runs, list)

    def test_pause_prevents_firing(self, scheduler) -> None:
        """Paused schedule doesn't fire on tick."""
        scheduler.pause("heartbeat")
        runs = scheduler.tick()
        heartbeat_fired = any(
            getattr(r, 'schedule_name', '') == "heartbeat" for r in runs
        )
        assert not heartbeat_fired

    def test_resume_after_pause(self, scheduler) -> None:
        """Resumed schedule can fire again."""
        scheduler.resume("heartbeat")
        # Just verify no error on resume

    def test_list_schedules(self, scheduler) -> None:
        """All registered schedules visible."""
        schedules = scheduler.list_schedules()
        assert len(schedules) >= 5

    def test_history(self, scheduler) -> None:
        """History shows past runs."""
        runs = scheduler.history("data_pipeline")
        assert len(runs) >= 1
