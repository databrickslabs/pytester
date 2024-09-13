from collections.abc import Callable
from unittest.mock import create_autospec

from databricks.labs.pytester.fixtures.unwrap import call_fixture
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementResponse, StatementState, StatementStatus

from databricks.labs.pytester.fixtures.baseline import ws, log_workspace_link, sql_backend


def test_ws() -> None:
    debug_env = {'DATABRICKS_HOST': 'abc', 'DATABRICKS_TOKEN': '...'}
    product_info = 'a', '0.1.2'
    workspace_client = call_fixture(ws, debug_env, product_info)

    assert workspace_client.config.hostname == 'abc'


def test_log_workspace_link():
    workspace_client = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-usage
    workspace_client.config.hostname = 'abc'
    logger = call_fixture(log_workspace_link, workspace_client)
    logger('name', 'path', anchor=True)


def test_sql_backend():
    workspace_client = create_autospec(WorkspaceClient)
    env_or_skip = create_autospec(Callable)

    workspace_client.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    backend = call_fixture(sql_backend, workspace_client, env_or_skip)
    backend.execute('SELECT 1')

    queries = [_.kwargs['statement'] for _ in workspace_client.statement_execution.method_calls]
    assert queries == ['SELECT 1']

    workspace_client.statement_execution.execute_statement.assert_called_once()
    env_or_skip.assert_called_once()
