import pytest
from unittest.mock import AsyncMock, patch
from click.testing import CliRunner
from mobilerun.cli.task_commands import tasks_group

def test_tasks_run_cli_parameter_passing():
    runner = CliRunner()
    
    with patch("mobilerun.cli.task_commands.async_run_orchestrator", new_callable=AsyncMock) as mock_orchestrator:
        result = runner.invoke(tasks_group, ["run", "Open Settings", "Toggle Flight Mode", "--at", "10", "--every", "60"])
        
        assert result.exit_code == 0
        mock_orchestrator.assert_called_once_with(["Open Settings", "Toggle Flight Mode"], 10, 60) 