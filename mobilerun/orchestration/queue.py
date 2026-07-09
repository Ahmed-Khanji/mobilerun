"""In-memory task queue for the orchestration layer.

The queue stores :class:`TaskRequest` objects as :class:`TaskRecord` entries and
hands them out one at a time via :meth:`TaskQueue.dequeue`, ordered by priority
and then submission time.

``dequeue`` waits on an :class:`asyncio.Condition` and wakes itself when the
earliest scheduled task is due, so the queue works on its own without an
external scheduler; ``wake()`` lets a scheduler nudge it earlier when needed.
All datetimes are timezone-aware (UTC), matching ``models``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from mobilerun.orchestration.models import (
    InvalidTransitionError,
    TaskRecord,
    TaskRequest,
    TaskResult,
    TaskStatus,
)

logger = logging.getLogger("mobilerun")


class TaskQueue:
    """In-memory, priority-ordered task queue with an async blocking dequeue."""

    def __init__(self) -> None:
        # Storage is kept behind this one attribute so it can be swapped for a
        # persistent backend later without touching the ordering logic.
        self._store: Dict[str, TaskRecord] = {}
        self._cond = asyncio.Condition()
        self._listeners: List[Callable[[TaskRecord], None]] = []

    def submit(self, request: TaskRequest) -> str:
        """Add a request to the queue as WAITING and return its id."""
        self._store[request.id] = TaskRecord(request=request, status=TaskStatus.WAITING)
        self.wake()
        return request.id

    def wake(self) -> None:
        """Wake any waiting :meth:`dequeue` so it re-checks for ready tasks.

        Notifying an ``asyncio.Condition`` requires holding its lock, which a
        synchronous caller cannot await, so the notify is scheduled on the loop.
        With no running loop there is nothing waiting, so this does nothing.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._notify())

    async def _notify(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    async def dequeue(self) -> TaskRequest:
        """Return the next ready task, waiting until one is available.

        A task is ready when it is WAITING and either has no ``scheduled_at`` or
        its ``scheduled_at`` has passed. Ready tasks are ordered by priority
        (highest first), then by creation time.
        """
        async with self._cond:
            while True:
                now = datetime.now(timezone.utc)
                record = self._next_ready(now)
                if record is not None:
                    return record.request
                # Wait until woken, or until the soonest scheduled task is due.
                # A timeout of None blocks until the next wake().
                timeout = self._seconds_until_next(now)
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout)
                except asyncio.TimeoutError:
                    pass

    def _next_ready(self, now: datetime) -> Optional[TaskRecord]:
        ready = [
            rec
            for rec in self._store.values()
            if rec.status == TaskStatus.WAITING and self._is_ready(rec.request, now)
        ]
        if not ready:
            return None
        return min(ready, key=lambda r: (-r.request.priority, r.request.created_at))

    @staticmethod
    def _is_ready(request: TaskRequest, now: datetime) -> bool:
        return request.scheduled_at is None or request.scheduled_at <= now

    def _seconds_until_next(self, now: datetime) -> Optional[float]:
        upcoming = [
            rec.request.scheduled_at
            for rec in self._store.values()
            if rec.status == TaskStatus.WAITING
            and rec.request.scheduled_at is not None
            and rec.request.scheduled_at > now
        ]
        if not upcoming:
            return None
        return max(0.0, (min(upcoming) - now).total_seconds())

    def mark_running(self, task_id: str) -> None:
        self._transition(task_id, {TaskStatus.WAITING}, TaskStatus.RUNNING)

    def mark_completed(self, task_id: str, result: TaskResult) -> None:
        self._transition(task_id, {TaskStatus.RUNNING}, TaskStatus.COMPLETED, result)

    def mark_failed(self, task_id: str, result: TaskResult) -> None:
        self._transition(task_id, {TaskStatus.RUNNING}, TaskStatus.FAILED, result)

    def _transition(
        self,
        task_id: str,
        allowed_from: set,
        to_status: TaskStatus,
        result: Optional[TaskResult] = None,
    ) -> None:
        record = self._store.get(task_id)
        if record is None:
            raise InvalidTransitionError(f"unknown task id: {task_id}")
        if record.status not in allowed_from:
            raise InvalidTransitionError(
                f"cannot move {task_id} from {record.status.value} "
                f"to {to_status.value}"
            )
        record.status = to_status
        if result is not None:
            record.result = result
        self._emit(record)

    def _emit(self, record: TaskRecord) -> None:
        for callback in list(self._listeners):
            try:
                callback(record)
            except Exception:
                logger.exception("task queue listener raised; ignoring")
