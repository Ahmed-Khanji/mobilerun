"""Shared data model for the task orchestration layer.

Datetimes are timezone-aware (UTC) throughout — ``TaskQueue.dequeue`` and the
``Scheduler`` compare ``scheduled_at`` against ``datetime.now(timezone.utc)``,
so every producer of a ``TaskRequest`` must supply aware datetimes too.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, Optional


class TaskStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class InvalidTransitionError(Exception):
    """Raised by TaskQueue when a status transition is not legal."""


@dataclass
class TaskRequest:
    """A single task to run through MobileAgent, plus queue/scheduling metadata."""

    goal: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    scheduled_at: Optional[datetime] = None
    repeat_every: Optional[timedelta] = None
    priority: int = 0
    device_serial: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    config_path: Optional[str] = None
    timeout: int = 1000
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["created_at"] = self.created_at.isoformat()
        result["scheduled_at"] = (
            self.scheduled_at.isoformat() if self.scheduled_at is not None else None
        )
        result["repeat_every"] = (
            self.repeat_every.total_seconds() if self.repeat_every is not None else None
        )
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskRequest":
        data = dict(data)
        if data.get("created_at") is not None:
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        if data.get("scheduled_at") is not None:
            data["scheduled_at"] = datetime.fromisoformat(data["scheduled_at"])
        if data.get("repeat_every") is not None:
            data["repeat_every"] = timedelta(seconds=data["repeat_every"])
        return cls(**data)


@dataclass
class TaskResult:
    """Flattened result of running a TaskRequest through MobileAgent."""

    task_id: str
    success: bool
    reason: str
    steps: int
    structured_output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["started_at"] = (
            self.started_at.isoformat() if self.started_at is not None else None
        )
        result["finished_at"] = (
            self.finished_at.isoformat() if self.finished_at is not None else None
        )
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        data = dict(data)
        if data.get("started_at") is not None:
            data["started_at"] = datetime.fromisoformat(data["started_at"])
        if data.get("finished_at") is not None:
            data["finished_at"] = datetime.fromisoformat(data["finished_at"])
        return cls(**data)


@dataclass
class TaskRecord:
    """What the queue stores and returns on queries."""

    request: TaskRequest
    status: TaskStatus = TaskStatus.WAITING
    result: Optional[TaskResult] = None
