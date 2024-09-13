import logging
import random
import string

from pytest import fixture

from databricks.sdk import WorkspaceClient
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
    # ignores fixed in https://github.com/databricks/databricks-sdk-py/pull/760
    return WorkspaceClient(
        host=debug_env["DATABRICKS_HOST"],
        auth_type=debug_env.get("DATABRICKS_AUTH_TYPE"),  # type: ignore
        token=debug_env.get("DATABRICKS_TOKEN"),  # type: ignore
        username=debug_env.get("DATABRICKS_USERNAME"),  # type: ignore
        password=debug_env.get("DATABRICKS_PASSWORD"),  # type: ignore
        client_id=debug_env.get("DATABRICKS_CLIENT_ID"),  # type: ignore
        client_secret=debug_env.get("DATABRICKS_CLIENT_SECRET"),  # type: ignore
        debug_truncate_bytes=debug_env.get("DATABRICKS_DEBUG_TRUNCATE_BYTES"),  # type: ignore
        debug_headers=debug_env.get("DATABRICKS_DEBUG_HEADERS"),  # type: ignore
        azure_client_id=debug_env.get("ARM_CLIENT_ID"),  # type: ignore
        azure_tenant_id=debug_env.get("ARM_TENANT_ID"),  # type: ignore
        azure_client_secret=debug_env.get("ARM_CLIENT_SECRET"),  # type: ignore
        cluster_id=debug_env.get("DATABRICKS_CLUSTER_ID"),  # type: ignore
        product=product_name,
        product_version=product_version,
    )


@fixture
def log_workspace_link(ws):
    """Returns a function to log a workspace link."""

    def inner(name: str, path: str, *, anchor: bool = True):
        a = '#' if anchor else ''
        url = f'https://{ws.config.hostname}/{a}{path}'
        _LOG.info(f'Created {name}: {url}')

    return inner
