INLINE = """
import pytest

from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementStatus, StatementState, StatementResponse


@pytest.fixture
def ws():  # noqa: F811
    some = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-assign
    some.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )
    return some

@pytest.fixture
def _is_in_debug() -> bool:
    return True

@pytest.fixture
def env_or_skip():
    def inner(n):
        return n
    return inner

def test_some(
    debug_env,
    make_job,
    make_directory,
    make_repo,
    make_model,
    make_experiment,
    make_serving_endpoint,
    make_secret_scope,
        sql_exec,
):
    make_job()
    sql_exec("SELECT 1")
"""


def test_a_thing(pytester):
    pytester.makepyfile(INLINE)
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)
