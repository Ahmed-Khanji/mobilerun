import asyncio
from datetime import datetime, timezone
import logging
from typing import Dict, Optional
from mobilerun.orchestration.models import TaskRequest, TaskStatus, TaskRecord
from mobilerun.orchestration.queue import TaskQueue

logger = logging.getLogger(__name__)

class Scheduler:
    """
    Handles absolute-time delayed tasks and interval-based recurrence profiles.
    Operates completely standard-library only using asyncio event loop timers.
    """
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self._active_timers: Dict[str, asyncio.Task] = {}
        self._running = False

    def schedule(self, request: TaskRequest) -> str:
        """Submits a task request to the queue and sets a timer if it is scheduled for the future."""
        task_id = self.queue.submit(request)
        
        now_utc = datetime.now(timezone.utc)
        sched_at = request.scheduled_at.astimezone(timezone.utc) if request.scheduled_at else None
        
        if sched_at and sched_at > now_utc:
            delay = (sched_at - now_utc).total_seconds()
            logger.info(f"[Orchestration] Scheduling task {task_id} to trigger in {delay:.2f}s.")
            timer_task = asyncio.create_task(self._wait_and_wake(task_id, delay))
            self._active_timers[task_id] = timer_task
        else:
            self.queue.wake()
            
        return task_id

    async def _wait_and_wake(self, task_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            logger.info(f"[Orchestration] Timer fired for delayed task: {task_id}")
            self.queue.wake()
        except asyncio.CancelledError:
            logger.debug(f"[Orchestration] Active timer cancelled for task: {task_id}")
        finally:
            self._active_timers.pop(task_id, None)

    async def start(self) -> None:
        """Starts background orchestration services and attaches listeners for recurrence processing."""
        if self._running:
            return
        self._running = True
        self.queue.add_listener(self._handle_recurrence_trigger)
        logger.info("[Orchestration] Scheduler engine started successfully.")

    def _handle_recurrence_trigger(self, record: TaskRecord) -> None:
        """C2 Implementation: Listens for completed tasks and clones them for their next interval run."""
        if not self._running:
            return

        if record.status == TaskStatus.COMPLETED and record.request.repeat_every:
            old_req = record.request
            next_run = datetime.now(timezone.utc) + old_req.repeat_every
            
            cloned_request = TaskRequest(
                goal=old_req.goal,
                scheduled_at=next_run,
                repeat_every=old_req.repeat_every,
                priority=old_req.priority,
                device_serial=old_req.device_serial,
                llm_provider=old_req.llm_provider,
                llm_model=old_req.llm_model,
                config_path=old_req.config_path,
                timeout=old_req.timeout,
                metadata=old_req.metadata.copy() if old_req.metadata else {}
            )
            logger.info(f"[Orchestration] Recurring rule match. Cloning task {old_req.id} for execution at {next_run}.")
            self.schedule(cloned_request)

    def stop(self) -> None:
        """Stops the scheduler loop and cleans up any armed pending execution tasks."""
        self._running = False
        for task_id, timer in list(self._active_timers.items()):
            timer.cancel()
        self._active_timers.clear()
        logger.info("[Orchestration] Scheduler engine stopped cleanly.")