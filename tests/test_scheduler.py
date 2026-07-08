import asyncio
import pytest
from datetime import datetime, timedelta, timezone
from mobilerun.orchestration.models import TaskRequest, TaskStatus, TaskResult
from mobilerun.orchestration.queue import TaskQueue
from mobilerun.orchestration.scheduler import Scheduler
from mobilerun.orchestration.triggers import TriggerManager

@pytest.mark.asyncio
async def test_scheduler_timing_gating():
    queue = TaskQueue()
    scheduler = Scheduler(queue)
    await scheduler.start()

    req1 = TaskRequest(goal="Turn on bluetooth")
    scheduler.schedule(req1)
    
    future_time = datetime.now(timezone.utc) + timedelta(minutes=2)
    req2 = TaskRequest(goal="Capture system view log", scheduled_at=future_time)
    scheduler.schedule(req2)

    first_up = await queue.dequeue()
    assert first_up.id == req1.id

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.dequeue(), timeout=0.1)

    scheduler.stop()

@pytest.mark.asyncio
async def test_recurrence_cloning_behavior():
    queue = TaskQueue()
    scheduler = Scheduler(queue)
    await scheduler.start()

    req = TaskRequest(goal="Clear RAM buffer cache", repeat_every=timedelta(seconds=1))
    t_id = scheduler.schedule(req)

    queue.mark_running(t_id)
    mock_res = TaskResult(task_id=t_id, success=True, reason="execution clear", steps=2)
    queue.mark_completed(t_id, mock_res)

    await asyncio.sleep(0.02)

    all_tasks = queue.list()
    assert len(all_tasks) == 2
    assert all_tasks[0].status == TaskStatus.COMPLETED
    assert all_tasks[1].status == TaskStatus.WAITING
    assert all_tasks[1].request.goal == "Clear RAM buffer cache"

    scheduler.stop()

@pytest.mark.asyncio
async def test_trigger_manager_reactive_cloning():
    queue = TaskQueue()
    scheduler = Scheduler(queue)
    trigger_mgr = TriggerManager(scheduler)
    
    template = TaskRequest(goal="Lock safe container logs", priority=10)
    trigger_mgr.register_trigger("LOW_BATTERY_EVENT", template)
    
    assert len(queue.list()) == 0
    
    trigger_mgr.fire_event("LOW_BATTERY_EVENT")
    
    records = queue.list()
    assert len(records) == 1
    assert records[0].request.goal == "Lock safe container logs"
    assert records[0].request.priority == 10 