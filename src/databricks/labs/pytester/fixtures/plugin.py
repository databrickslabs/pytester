import pytest

# pylint: disable=wildcard-import,unused-wildcard-import
from databricks.labs.pytester.fixtures import *  # noqa: F403, F401


def pytest_addoption(parser):
    group = parser.getgroup("helloworld")
    group.addoption(
        "--name",
        action="store",
        dest="name",
        default="World",
        help='Default "name" for hello().',
    )


@pytest.fixture
def hello(request):
    def _hello(name=None):
        if not name:
            name = request.config.getoption("name")
        return f"Hello {name}!"

    return _hello
