import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from mobilerun.orchestration.models import (
    InvalidTransitionError,
    TaskRecord,
    TaskRequest,
    TaskResult,
    TaskStatus,
)
from mobilerun.orchestration.queue import TaskQueue


def make_result(task_id: str, success: bool = True) -> TaskResult:
    return TaskResult(task_id=task_id, success=success, reason="done", steps=1)


def test_dequeue_returns_tasks_in_fifo_order() -> None:
    queue = TaskQueue()
    first = TaskRequest(goal="first")
    second = TaskRequest(goal="second")
    queue.submit(first)
    queue.submit(second)

    assert asyncio.run(queue.dequeue()).id == first.id


def test_higher_priority_dequeued_first() -> None:
    queue = TaskQueue()
    low = TaskRequest(goal="low", priority=0)
    high = TaskRequest(goal="high", priority=5)
    queue.submit(low)
    queue.submit(high)

    assert asyncio.run(queue.dequeue()).id == high.id


def test_dequeue_orders_by_priority_then_creation() -> None:
    queue = TaskQueue()
    a = TaskRequest(goal="a", priority=1)
    b = TaskRequest(goal="b", priority=1)
    c = TaskRequest(goal="c", priority=2)
    for request in (a, b, c):
        queue.submit(request)

    order = []
    for _ in range(3):
        request = asyncio.run(queue.dequeue())
        queue.mark_running(request.id)  # so it is not picked again
        order.append(request.id)

    assert order == [c.id, a.id, b.id]


def test_dequeue_blocks_until_a_task_is_submitted() -> None:
    async def scenario() -> str:
        queue = TaskQueue()
        waiter = asyncio.ensure_future(queue.dequeue())
        await asyncio.sleep(0.02)
        assert not waiter.done()

        queue.submit(TaskRequest(goal="late"))
        return (await asyncio.wait_for(waiter, timeout=1.0)).id

    assert asyncio.run(scenario())


def test_wake_with_no_waiter_does_nothing() -> None:
    TaskQueue().wake()


def test_wake_rechecks_readiness() -> None:
    async def scenario() -> str:
        queue = TaskQueue()
        waiter = asyncio.ensure_future(queue.dequeue())
        await asyncio.sleep(0.02)
        assert not waiter.done()

        request = TaskRequest(goal="injected")
        queue._store[request.id] = TaskRecord(request=request)
        queue.wake()
        return (await asyncio.wait_for(waiter, timeout=1.0)).id

    assert asyncio.run(scenario())


def test_future_task_is_not_dequeued_early() -> None:
    async def scenario() -> bool:
        queue = TaskQueue()
        later = datetime.now(timezone.utc) + timedelta(seconds=10)
        queue.submit(TaskRequest(goal="later", scheduled_at=later))

        waiter = asyncio.ensure_future(queue.dequeue())
        await asyncio.sleep(0.05)
        blocked = not waiter.done()
        waiter.cancel()
        return blocked

    assert asyncio.run(scenario()) is True


def test_scheduled_task_becomes_ready_on_time() -> None:
    async def scenario() -> str:
        queue = TaskQueue()
        soon = datetime.now(timezone.utc) + timedelta(seconds=0.05)
        queue.submit(TaskRequest(goal="due soon", scheduled_at=soon))
        return (await asyncio.wait_for(queue.dequeue(), timeout=1.0)).id

    assert asyncio.run(scenario())


def test_ready_task_preferred_over_future_task() -> None:
    async def scenario() -> str:
        queue = TaskQueue()
        later = datetime.now(timezone.utc) + timedelta(seconds=10)
        queue.submit(TaskRequest(goal="later", scheduled_at=later, priority=9))
        ready = TaskRequest(goal="now", priority=0)
        queue.submit(ready)
        return (await asyncio.wait_for(queue.dequeue(), timeout=1.0)).id

    assert asyncio.run(scenario())


def test_full_transition_chain() -> None:
    queue = TaskQueue()
    request = TaskRequest(goal="ok")
    queue.submit(request)
    queue.mark_running(request.id)
    queue.mark_completed(request.id, make_result(request.id))

    record = queue._store[request.id]
    assert record.status == TaskStatus.COMPLETED
    assert record.result is not None


def test_mark_running_requires_waiting() -> None:
    queue = TaskQueue()
    request = TaskRequest(goal="x")
    queue.submit(request)
    queue.mark_running(request.id)

    with pytest.raises(InvalidTransitionError):
        queue.mark_running(request.id)


def test_mark_completed_requires_running() -> None:
    queue = TaskQueue()
    request = TaskRequest(goal="x")
    queue.submit(request)

    with pytest.raises(InvalidTransitionError):
        queue.mark_completed(request.id, make_result(request.id))


def test_mark_failed_requires_running() -> None:
    queue = TaskQueue()
    request = TaskRequest(goal="x")
    queue.submit(request)

    with pytest.raises(InvalidTransitionError):
        queue.mark_failed(request.id, make_result(request.id, success=False))


def test_transition_on_unknown_task_raises() -> None:
    with pytest.raises(InvalidTransitionError):
        TaskQueue().mark_running("missing")


def test_listener_error_does_not_stop_other_listeners() -> None:
    queue = TaskQueue()
    seen = []

    def broken(record):
        raise RuntimeError("boom")

    queue._listeners.append(broken)
    queue._listeners.append(lambda record: seen.append(record.status))

    request = TaskRequest(goal="x")
    queue.submit(request)
    queue.mark_running(request.id)

    assert seen == [TaskStatus.RUNNING]
