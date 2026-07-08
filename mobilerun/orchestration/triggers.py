import logging
from typing import Dict, List, Callable
from mobilerun.orchestration.models import TaskRequest
from mobilerun.orchestration.scheduler import Scheduler

logger = logging.getLogger(__name__)

class TriggerManager:
    """Manages simple reactive rules mapping environmental events to workflow triggers."""
    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self._registry: Dict[str, List[TaskRequest]] = {}

    def register_trigger(self, event_name: str, task_template: TaskRequest) -> None:
        """Binds a specific runtime event to a blueprint task request template."""
        if event_name not in self._registry:
            self._registry[event_name] = []
        self._registry[event_name].append(task_template)
        logger.info(f"[Orchestration] Registered event trigger template for event: '{event_name}'")

    def fire_event(self, event_name: str) -> None:
        """Evaluates registered criteria and spins up instance clones immediately when invoked."""
        if event_name not in self._registry:
            return
        
        logger.info(f"[Orchestration] Event handler caught active signal: '{event_name}'")
        for template in self._registry[event_name]:
            cloned_req = TaskRequest(
                goal=template.goal,
                priority=template.priority,
                device_serial=template.device_serial,
                llm_provider=template.llm_provider,
                llm_model=template.llm_model,
                config_path=template.config_path,
                timeout=template.timeout,
                metadata=template.metadata.copy() if template.metadata else {}
            )
            self.scheduler.schedule(cloned_req)