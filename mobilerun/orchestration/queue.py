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

    def get(self, task_id: str) -> TaskRecord | None:
        """Return the TaskRecord with the given task_id, or return None if it doesn't exist"""
        return self._store.get(task_id)

    def list(self, status: TaskStatus | None = None) -> list[TaskRecord]:
        "Return a list of TaskRecord with the given status, or return a list of all TaskRecord if the status is None"""
        records = list(self._store.values())

        if status is None:
            return records
        else:
            return [r for r in records if r.status == status]

    def cancel(self, task_id: str) -> bool:
        """Cancel a task. Returns true if a task is actually cancelled. Only cancel the task if it's status is WAITING"""
        try:
            self._transition(task_id, {TaskStatus.WAITING}, TaskStatus.CANCELLED)
            return True
        except InvalidTransitionError:
            return False
        
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

    def add_listener(self, cb: Callable[[TaskRecord], None]) -> None:
        """Register a callback fired on every status change(mark_running, mark_completed, mark_failed, cancel)"""
        self._listeners.append(cb)

    def _emit(self, record: TaskRecord) -> None:
        for callback in list(self._listeners):
            try:
                callback(record)
            except Exception:
                logger.exception("task queue listener raised; ignoring")

