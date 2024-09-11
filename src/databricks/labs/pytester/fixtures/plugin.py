import pytest

from databricks.labs.pytester.fixtures.baseline import (
    ws,
    make_random,
    sql_backend,
    sql_exec,
    sql_fetch_all,
    product_info,
)
from databricks.labs.pytester.fixtures.compute import make_instance_pool, make_job, make_cluster, make_cluster_policy
from databricks.labs.pytester.fixtures.iam import make_group, make_user
from databricks.labs.pytester.fixtures.catalog import make_udf, make_catalog, make_schema, make_table
from databricks.labs.pytester.fixtures.notebooks import make_notebook, make_directory, make_repo
from databricks.labs.pytester.fixtures.secrets import make_secret_scope, make_secret_scope_acl
from databricks.labs.pytester.fixtures.wheel import workspace_library
from databricks.labs.pytester.fixtures.environment import debug_env, debug_env_name, env_or_skip

__all__ = [
    'debug_env_name',
    'debug_env',
    'env_or_skip',
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
    'make_udf',
    'make_catalog',
    'make_schema',
    'make_table',
    'product_info',
    'sql_backend',
    'sql_exec',
    'sql_fetch_all',
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
