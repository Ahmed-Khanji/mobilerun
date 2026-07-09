from datetime import datetime, timedelta, timezone

import pytest

from mobilerun.orchestration.models import (
    InvalidTransitionError,
    TaskRecord,
    TaskRequest,
    TaskResult,
    TaskStatus,
)


def test_task_request_defaults() -> None:
    request = TaskRequest(goal="open settings")

    assert request.goal == "open settings"
    assert isinstance(request.id, str) and request.id
    assert request.created_at.tzinfo is not None
    assert request.scheduled_at is None
    assert request.repeat_every is None
    assert request.priority == 0
    assert request.timeout == 1000
    assert request.metadata == {}


def test_task_request_defaults_are_independent_per_instance() -> None:
    first = TaskRequest(goal="a")
    second = TaskRequest(goal="b")

    assert first.id != second.id

    first.metadata["key"] = "value"
    assert second.metadata == {}


def test_task_request_to_dict_from_dict_round_trip_with_optional_fields() -> None:
    request = TaskRequest(
        goal="turn on dark mode",
        scheduled_at=datetime(2026, 7, 8, 21, 0, tzinfo=timezone.utc),
        repeat_every=timedelta(minutes=30),
        priority=5,
        device_serial="emulator-5554",
        llm_provider="Anthropic",
        llm_model="claude-sonnet-5",
        config_path="config/custom.yaml",
        metadata={"source": "cli"},
    )

    restored = TaskRequest.from_dict(request.to_dict())

    assert restored == request


def test_task_request_to_dict_from_dict_round_trip_with_none_optionals() -> None:
    request = TaskRequest(goal="check wifi")

    restored = TaskRequest.from_dict(request.to_dict())

    assert restored == request
    assert restored.scheduled_at is None
    assert restored.repeat_every is None


def test_task_result_to_dict_from_dict_round_trip() -> None:
    result = TaskResult(
        task_id="abc123",
        success=True,
        reason="done",
        steps=4,
        structured_output={"answer": 42},
        error=None,
        started_at=datetime(2026, 7, 8, 21, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 8, 21, 1, tzinfo=timezone.utc),
    )

    restored = TaskResult.from_dict(result.to_dict())

    assert restored == result


def test_task_result_to_dict_from_dict_round_trip_with_none_optionals() -> None:
    result = TaskResult(task_id="abc123", success=False, reason="failed", steps=0)

    restored = TaskResult.from_dict(result.to_dict())

    assert restored == result
    assert restored.started_at is None
    assert restored.finished_at is None


def test_task_status_values() -> None:
    assert TaskStatus.WAITING == "waiting"
    assert TaskStatus.RUNNING == "running"
    assert TaskStatus.COMPLETED == "completed"
    assert TaskStatus.FAILED == "failed"
    assert TaskStatus.CANCELLED == "cancelled"


def test_task_record_defaults() -> None:
    record = TaskRecord(request=TaskRequest(goal="open settings"))

    assert record.status == TaskStatus.WAITING
    assert record.result is None


def test_invalid_transition_error_is_an_exception() -> None:
    with pytest.raises(InvalidTransitionError):
        raise InvalidTransitionError("bad transition")
