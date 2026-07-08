from datetime import datetime, timedelta, timezone
import sys
import asyncio
import click

from mobilerun.orchestration.models import TaskRequest, TaskStatus, TaskRecord
from mobilerun.orchestration.queue import TaskQueue
from mobilerun.orchestration.runner import TaskRunner, MobileAgentInvoker
from mobilerun.orchestration.scheduler import Scheduler

@click.group(name="tasks")
def tasks_group():
    """Manage and execute multi-task automation workflows for mobile environments."""
    pass

def draw_monitoring_grid(records: list[TaskRecord]):
    """Clears the console screen and draws a scannable ASCII grid tracking active workloads."""
    click.clear()
    click.echo(f"================== MOBILERUN WORKFLOW MANAGER ({datetime.now().strftime('%H:%M:%S')}) ==================")
    click.echo(f"{'TASK ID':<10} | {'AUTOMATION OBJECTIVE / GOAL':<35} | {'STATUS':<12} | {'METRICS / CONTEXT'}")
    click.echo("-" * 95)
    
    for rec in records:
        context_str = ""
        if rec.status == TaskStatus.COMPLETED and rec.result:
            context_str = f"Steps: {rec.result.steps} | Reason: {rec.result.reason}"
        elif rec.status == TaskStatus.FAILED and rec.result:
            context_str = f"Exception/Failure: {rec.result.error or rec.result.reason}"
        elif rec.request.scheduled_at:
            time_left = (rec.request.scheduled_at.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()
            context_str = f"Execution countdown: {max(0.0, time_left):.1f}s"
            
        click.echo(f"{rec.request.id[:8]:<10} | {rec.request.goal[:33]:<35} | {rec.status.value.upper():<12} | {context_str}")
    click.echo("\n[System Running] Press Ctrl+C at any time to gracefully terminate all queues.")

async def async_run_orchestrator(goals: list[str], delay_sec: int, repeat_sec: int):
    """Wires up components in-process, populates requirements, and loops until the tasks complete."""
    queue = TaskQueue()
    scheduler = Scheduler(queue)
    invoker = MobileAgentInvoker()
    runner = TaskRunner(queue, invoker)

    await scheduler.start()
    runner_task = asyncio.create_task(runner.run_forever())

    target_time = datetime.now(timezone.utc) + timedelta(seconds=delay_sec) if delay_sec > 0 else None
    interval_delta = timedelta(seconds=repeat_sec) if repeat_sec > 0 else None

    for goal_string in goals:
        req = TaskRequest(
            goal=goal_string,
            scheduled_at=target_time,
            repeat_every=interval_delta
        )
        scheduler.schedule(req)

    try:
        while True:
            records = queue.list()
            draw_monitoring_grid(records)
            
            terminal_states = [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
            work_complete = all(r.status in terminal_states for r in records)
            
            if work_complete and not interval_delta:
                break
                
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        pass
    finally:
        scheduler.stop()
        runner.stop()
        await runner_task

    final_snap = queue.list()
    if any(r.status == TaskStatus.FAILED for r in final_snap):
        sys.exit(1)
    sys.exit(0)

@tasks_group.command(name="run")
@click.argument("goals", nargs=-1, required=True)
@click.option("--at", "delay_sec", type=int, default=0, help="Delays task initiation window by N seconds.")
@click.option("--every", "repeat_sec", type=int, default=0, help="Loops task runs on repeating intervals of N seconds.")
def run_tasks(goals, delay_sec, repeat_sec):
    """Execute sequence or interval-driven automation goals over terminal processes."""
    try:
        asyncio.run(async_run_orchestrator(list(goals), delay_sec, repeat_sec))
    except KeyboardInterrupt:
        click.echo("\n[Warning] Manual execution signal interrupt caught. Evacuating runtimes.")
        sys.exit(130)