import logging
import random
import string
from collections.abc import Callable, Generator
from typing import Any, TypeVar

from pytest import fixture

from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.config import Config
from databricks.sdk.errors import DatabricksError

_LOG = logging.getLogger(__name__)


@fixture
def make_random():
    """
    Fixture to generate random strings.

    This fixture provides a function to generate random strings of a specified length.
    The generated strings are created using a character set consisting of uppercase letters,
    lowercase letters, and digits.

    To generate a random string with default length of 16 characters:

    ```python
    random_string = make_random()
    assert len(random_string) == 16
    ```

    To generate a random string with a specified length:

    ```python
    random_string = make_random(k=8)
    assert len(random_string) == 8
    ```
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


T = TypeVar("T")


def factory(
    name: str,
    create: Callable[..., T],
    remove: Callable[[T], None],
) -> Generator[Callable[..., T], None, None]:
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
    cleanup: list[T] = []

    def inner(**kwargs: Any) -> T:
        out = create(**kwargs)
        _LOG.debug(f"added {name} fixture: {out}")
        cleanup.append(out)
        return out

    yield inner
    _LOG.debug(f"clearing {len(cleanup)} {name} fixtures")
    for some in cleanup:
        try:
            _LOG.debug(f"removing {name} fixture: {some}")
            remove(some)
        except DatabricksError as e:
            _LOG.debug(f"ignoring error while {name} {some} teardown: {e}")


@fixture
def product_info():
    return None, None


@fixture
def ws(debug_env: dict[str, str], product_info: tuple[str, str]) -> WorkspaceClient:
    """
    Create and provide a Databricks WorkspaceClient object.

    This fixture initializes a Databricks WorkspaceClient object, which can be used
    to interact with the Databricks workspace API. The created instance of WorkspaceClient
    is shared across all test functions within the test session.

    See [detailed documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html) for the list
    of environment variables that can be used to authenticate the WorkspaceClient.

    In your test functions, include this fixture as an argument to use the WorkspaceClient:

    ```python
    def test_workspace_operations(ws):
        clusters = ws.clusters.list_clusters()
        assert len(clusters) >= 0
    ```
    """
    product_name, product_version = product_info
    return WorkspaceClient(
        host=debug_env["DATABRICKS_HOST"],
        auth_type=debug_env.get("DATABRICKS_AUTH_TYPE"),
        token=debug_env.get("DATABRICKS_TOKEN"),
        username=debug_env.get("DATABRICKS_USERNAME"),
        password=debug_env.get("DATABRICKS_PASSWORD"),
        client_id=debug_env.get("DATABRICKS_CLIENT_ID"),
        client_secret=debug_env.get("DATABRICKS_CLIENT_SECRET"),
        debug_truncate_bytes=debug_env.get("DATABRICKS_DEBUG_TRUNCATE_BYTES"),  # type: ignore
        debug_headers=debug_env.get("DATABRICKS_DEBUG_HEADERS"),  # type: ignore
        azure_client_id=debug_env.get("ARM_CLIENT_ID"),
        azure_tenant_id=debug_env.get("ARM_TENANT_ID"),
        azure_client_secret=debug_env.get("ARM_CLIENT_SECRET"),
        cluster_id=debug_env.get("DATABRICKS_CLUSTER_ID"),
        product=product_name,
        product_version=product_version,
    )


@fixture
def acc(debug_env: dict[str, str], product_info: tuple[str, str], env_or_skip) -> AccountClient:
    """
    Create and provide a Databricks AccountClient object.

    This fixture initializes a Databricks AccountClient object, which can be used
    to interact with the Databricks account API. The created instance of AccountClient
    is shared across all test functions within the test session.

    Requires `DATABRICKS_ACCOUNT_ID` environment variable to be set. If `DATABRICKS_HOST`
    points to a workspace host, the fixture would automatically determine the account host
    from it.

    See [detailed documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html) for the list
    of environment variables that can be used to authenticate the AccountClient.

    In your test functions, include this fixture as an argument to use the AccountClient:

    ```python
    def test_listing_workspaces(acc):
        workspaces = acc.workspaces.list()
        assert len(workspaces) >= 1
    ```
    """
    product_name, product_version = product_info
    config = Config(
        host=debug_env["DATABRICKS_HOST"],
        account_id=env_or_skip("DATABRICKS_ACCOUNT_ID"),
        auth_type=debug_env.get("DATABRICKS_AUTH_TYPE"),
        username=debug_env.get("DATABRICKS_USERNAME"),
        password=debug_env.get("DATABRICKS_PASSWORD"),
        client_id=debug_env.get("DATABRICKS_CLIENT_ID"),
        client_secret=debug_env.get("DATABRICKS_CLIENT_SECRET"),
        debug_truncate_bytes=debug_env.get("DATABRICKS_DEBUG_TRUNCATE_BYTES"),  # type: ignore
        debug_headers=debug_env.get("DATABRICKS_DEBUG_HEADERS"),  # type: ignore
        azure_client_id=debug_env.get("ARM_CLIENT_ID"),
        azure_tenant_id=debug_env.get("ARM_TENANT_ID"),
        azure_client_secret=debug_env.get("ARM_CLIENT_SECRET"),
        product=product_name,
        product_version=product_version,
    )
    config.host = config.environment.deployment_url('accounts')
    return AccountClient(config=config)


@fixture
def log_workspace_link(ws):
    """Returns a function to log a workspace link."""

    def inner(name: str, path: str, *, anchor: bool = True):
        a = '#' if anchor else ''
        url = f'https://{ws.config.hostname}/{a}{path}'
        _LOG.info(f'Created {name}: {url}')

    return inner


@fixture
def log_account_link(acc):
    """Returns a function to log an account link."""

    def inner(name: str, path: str, *, anchor: bool = False):
        a = '#' if anchor else ''
        url = f'https://{acc.config.hostname}/{a}{path}'
        _LOG.info(f'Created {name}: {url}')

    return inner
