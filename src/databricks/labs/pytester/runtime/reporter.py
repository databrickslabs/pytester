import pytest

from _pytest.config import Config, ExitCode
from _pytest.nodes import Item
from _pytest.terminal import TerminalReporter

class DatabricksRuntimePlugin:
    def __init__(self):
        pass

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item: Item):
        """Measure test execution duration."""
        pass

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item: "Item"):
        pass

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item: "Item"):
        pass

    def pytest_terminal_summary(
            self,
            terminalreporter: TerminalReporter,
            exitstatus: ExitCode,
            config: Config,
    ):
        pass
