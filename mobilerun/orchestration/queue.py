from mobilerun.orchestration.models import (
    InvalidTransitionError,
    TaskRecord,
    TaskRequest,
    TaskResult,
    TaskStatus
 )

from typing import Callable
import logging

class TaskQueue:

    def __init__(self):
        self._store: dict[str, TaskRecord] = {}
        self._listeners: list[Callable[[TaskRecord], None]] = []

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
        record = self._store.get(task_id)
        if record is None or record.status != TaskStatus.WAITING:
            return False

        record.status = TaskStatus.CANCELLED
        self._notify_listeners(record)
        return True

    def add_listener(self, cb: Callable[[TaskRecord], None]) -> None:
        """Register a callback fired on every status change(mark_running, mark_completed, mark_failed, cancel)"""
        self._listeners.append(cb)

    def _notify_listeners(self, record: TaskRecord) -> None:
        """Fire all registered listeners with the updated record."""
        for cb in self._listeners:
            try:
                cb(record)
            except Exception:
                logging.exception(f"Failed to fire the listener: {record}")

