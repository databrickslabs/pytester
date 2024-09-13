from functools import partial

from pytest import fixture
from databricks.labs.lsql.backends import StatementExecutionBackend


@fixture
def sql_backend(ws, env_or_skip) -> StatementExecutionBackend:
    """
    Create and provide a SQL backend for executing statements.

    Requires the environment variable `DATABRICKS_WAREHOUSE_ID` to be set.
    """
    warehouse_id = env_or_skip("DATABRICKS_WAREHOUSE_ID")
    return StatementExecutionBackend(ws, warehouse_id)


@fixture
def sql_exec(sql_backend):
    """
    Execute SQL statement and don't return any results.
    """
    return partial(sql_backend.execute)


@fixture
def sql_fetch_all(sql_backend):
    """
    Fetch all rows from a SQL statement.
    """
    return partial(sql_backend.fetch)
