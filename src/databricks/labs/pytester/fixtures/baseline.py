import json
import logging
import os
import pathlib
import random
import string
import sys
from collections.abc import Callable, MutableMapping
from datetime import timedelta, datetime, timezone
from functools import partial

import pytest
from pytest import fixture

from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

_LOG = logging.getLogger(__name__)


"""Preserve resources created during tests for at least this long."""
TEST_RESOURCE_PURGE_TIMEOUT = timedelta(hours=1)


@fixture
def make_random():
    """
    Fixture to generate random strings.

    This fixture provides a function to generate random strings of a specified length.
    The generated strings are created using a character set consisting of uppercase letters,
    lowercase letters, and digits.

    Returns:
    --------
    function:
        A function to generate random strings.

    Usage Example:
    --------------
    To generate a random string with default length of 16 characters:

    .. code-block:: python

       random_string = make_random()
       assert len(random_string) == 16

    To generate a random string with a specified length:

    .. code-block:: python

       random_string = make_random(k=8)
       assert len(random_string) == 8
    """

    def inner(k=16) -> str:
        """
        Generate a random string.

        Parameters:
        -----------
        k : int, optional
            Length of the random string (default is 16).

        Returns:
        --------
        str:
            A randomly generated string.
        """
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=int(k)))

    return inner


def _is_in_debug() -> bool:
    return os.path.basename(sys.argv[0]) in {"_jb_pytest_runner.py", "testlauncher.py"}


@fixture
def debug_env_name():
    # Alternatively, we could use @pytest.mark.xxx, but
    # not sure how reusable it becomes then.
    #
    # we don't use scope=session, as monkeypatch.setenv
    # doesn't work on a session level
    return "UNKNOWN"


@pytest.fixture
def debug_env(monkeypatch, debug_env_name) -> MutableMapping[str, str]:
    if not _is_in_debug():
        return os.environ
    # TODO: add support for `.env` files
    conf_file = pathlib.Path.home() / ".databricks/debug-env.json"
    if not conf_file.exists():
        return os.environ
    with conf_file.open("r") as f:
        conf = json.load(f)
        if debug_env_name not in conf:
            sys.stderr.write(f"""{debug_env_name} not found in ~/.databricks/debug-env.json""")
            msg = f"{debug_env_name} not found in ~/.databricks/debug-env.json"
            raise KeyError(msg)
        for env_key, value in conf[debug_env_name].items():
            monkeypatch.setenv(env_key, value)
    return os.environ


@fixture
def env_or_skip(debug_env) -> Callable[[str], str]:
    skip = pytest.skip
    if _is_in_debug():
        skip = pytest.fail  # type: ignore[assignment]

    def inner(var: str) -> str:
        if var not in debug_env:
            skip(f"Environment variable {var} is missing")
        return debug_env[var]

    return inner


def factory(name, create, remove):
    """
    Factory function for creating fixtures.

    This function creates a fixture for managing resources (e.g., secret scopes) within test functions.
    The provided ``create`` function is used to create a resource, and the provided ``remove`` function
    is used to remove the resource after the test is complete.

    Parameters:
    -----------
    name : str
        The name of the resource (e.g., "secret scope").
    create : function
        A function to create the resource.
    remove : function
        A function to remove the resource.

    Returns:
    --------
    function:
        A function to create and manage the resource fixture.

    Usage Example:
    --------------
    To create a fixture for managing secret scopes:

    .. code-block:: python

       @pytest.fixture
       def make_secret_scope(ws, make_random):
           def create(**kwargs):
               name = f"sdk-{make_random(4)}"
               ws.secrets.create_scope(name, **kwargs)
               return name

           def cleanup(scope):
               ws.secrets.delete_scope(scope)

           yield from factory("secret scope", create, cleanup)
    """
    cleanup = []

    def inner(**kwargs):
        some = create(**kwargs)
        _LOG.debug(f"added {name} fixture: {some}")
        cleanup.append(some)
        return some

    yield inner
    _LOG.debug(f"clearing {len(cleanup)} {name} fixtures")
    for some in cleanup:
        try:
            _LOG.debug(f"removing {name} fixture: {some}")
            remove(some)
        except NotFound as e:
            _LOG.debug(f"ignoring error while {name} {some} teardown: {e}")


@fixture
def product_info():
    return None, None


@fixture
def ws(debug_env, product_info) -> WorkspaceClient:
    """
    Create and provide a Databricks WorkspaceClient object.

    This fixture initializes a Databricks WorkspaceClient object, which can be used
    to interact with the Databricks workspace API. The created instance of WorkspaceClient
    is shared across all test functions within the test session.

    See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html

    Returns:
    --------
    databricks.sdk.WorkspaceClient:
        An instance of WorkspaceClient for interacting with the Databricks Workspace APIs.

    Usage:
    ------
    In your test functions, include this fixture as an argument to use the WorkspaceClient:

    .. code-block:: python

        def test_workspace_operations(ws):
            clusters = ws.clusters.list_clusters()
            assert len(clusters) >= 0
    """
    product_name, product_version = product_info
    return WorkspaceClient(host=debug_env["DATABRICKS_HOST"], product=product_name, product_version=product_version)


@fixture
def sql_backend(ws, env_or_skip) -> StatementExecutionBackend:
    """Create and provide a SQL backend for executing statements."""
    warehouse_id = env_or_skip("DATABRICKS_WAREHOUSE_ID")
    return StatementExecutionBackend(ws, warehouse_id)


@fixture
def sql_exec(sql_backend):
    """Execute SQL statement and don't return any results."""
    return partial(sql_backend.execute)


@fixture
def sql_fetch_all(sql_backend):
    """Fetch all rows from a SQL statement."""
    return partial(sql_backend.fetch)


def get_test_purge_time(timeout: timedelta = TEST_RESOURCE_PURGE_TIMEOUT) -> str:
    """Purge time for test objects, representing the (UTC-based) hour from which objects may be purged."""
    # Note: this code is duplicated in the workflow installer (WorkflowsDeployment) so that it can avoid the
    # transitive pytest deployment from this module.
    now = datetime.now(timezone.utc)
    purge_deadline = now + timeout
    # Round UP to the next hour boundary: that is when resources will be deleted.
    purge_hour = purge_deadline + (datetime.min.replace(tzinfo=timezone.utc) - purge_deadline) % timedelta(hours=1)
    return purge_hour.strftime("%Y%m%d%H")


def get_purge_suffix() -> str:
    """HEX-encoded purge time suffix for test objects."""
    return f'ra{int(get_test_purge_time()):x}'
