import pytest

from databricks.labs.pytester.fixtures.baseline import ws, make_random
from databricks.labs.pytester.fixtures.compute import make_instance_pool, make_job, make_cluster, make_cluster_policy
from databricks.labs.pytester.fixtures.iam import make_group, make_user
from databricks.labs.pytester.fixtures.notebooks import make_notebook, make_directory, make_repo
from databricks.labs.pytester.fixtures.secrets import make_secret_scope, make_secret_scope_acl
from databricks.labs.pytester.fixtures.wheel import workspace_library

__all__ = [
    'ws',
    'make_random',
    'make_instance_pool',
    'make_job',
    'make_cluster',
    'make_cluster_policy',
    'make_group',
    'make_user',
    'make_notebook',
    'make_directory',
    'make_repo',
    'make_secret_scope',
    'make_secret_scope_acl',
    'workspace_library',
]


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
