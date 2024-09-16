import pytest

from databricks.labs.pytester.fixtures.baseline import (
    ws,
    make_random,
    product_info,
    log_workspace_link,
)
from databricks.labs.pytester.fixtures.sql import sql_backend, sql_exec, sql_fetch_all
from databricks.labs.pytester.fixtures.compute import (
    make_instance_pool,
    make_job,
    make_cluster,
    make_cluster_policy,
    make_pipeline,
    make_warehouse,
)
from databricks.labs.pytester.fixtures.iam import make_group, make_user
from databricks.labs.pytester.fixtures.catalog import (
    make_udf,
    make_catalog,
    make_schema,
    make_table,
    make_storage_credential,
)
from databricks.labs.pytester.fixtures.notebooks import make_notebook, make_directory, make_repo
from databricks.labs.pytester.fixtures.permissions import (  # noqa
    make_cluster_policy_permissions,
    make_instance_pool_permissions,
    make_cluster_permissions,
    make_pipeline_permissions,
    make_job_permissions,
    make_notebook_permissions,
    make_directory_permissions,
    make_workspace_file_permissions,
    make_workspace_file_path_permissions,
    make_repo_permissions,
    make_authorization_permissions,
    make_warehouse_permissions,
    make_lakeview_dashboard_permissions,
    make_dashboard_permissions,
    make_alert_permissions,
    make_query_permissions,
    make_experiment_permissions,
    make_registered_model_permissions,
    make_serving_endpoint_permissions,
    make_feature_table_permissions,
)
from databricks.labs.pytester.fixtures.secrets import make_secret_scope, make_secret_scope_acl
from databricks.labs.pytester.fixtures.environment import debug_env, debug_env_name, env_or_skip, is_in_debug
from databricks.labs.pytester.fixtures.ml import make_experiment, make_model, make_serving_endpoint
from databricks.labs.pytester.fixtures.redash import make_query
from databricks.labs.pytester.fixtures.watchdog import watchdog_remove_after, watchdog_purge_suffix
from databricks.labs.pytester.fixtures.connect import spark

__all__ = [
    'debug_env_name',
    'debug_env',
    'env_or_skip',
    'ws',
    'spark',
    'sql_backend',
    'sql_exec',
    'sql_fetch_all',
    'make_random',
    'make_instance_pool',
    'make_instance_pool_permissions',
    'make_job',
    'make_job_permissions',
    'make_cluster',
    'make_cluster_permissions',
    'make_cluster_policy',
    'make_cluster_policy_permissions',
    'make_pipeline',
    'make_warehouse',
    'make_group',
    'make_user',
    'make_pipeline_permissions',
    'make_notebook',
    'make_notebook_permissions',
    'make_directory',
    'make_directory_permissions',
    'make_repo',
    'make_repo_permissions',
    'make_workspace_file_permissions',
    'make_workspace_file_path_permissions',
    'make_secret_scope',
    'make_secret_scope_acl',
    'make_authorization_permissions',
    'make_udf',
    'make_catalog',
    'make_schema',
    'make_table',
    'make_storage_credential',
    'product_info',
    'make_model',
    'make_experiment',
    'make_experiment_permissions',
    'make_warehouse_permissions',
    'make_lakeview_dashboard_permissions',
    'log_workspace_link',
    'make_dashboard_permissions',
    'make_alert_permissions',
    'make_query',
    'make_query_permissions',
    'make_registered_model_permissions',
    'make_serving_endpoint',
    'make_serving_endpoint_permissions',
    'make_feature_table_permissions',
    'watchdog_remove_after',
    'watchdog_purge_suffix',
    'is_in_debug',
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
