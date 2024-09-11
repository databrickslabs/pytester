import logging
import random
import string

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

_LOG = logging.getLogger(__name__)


@pytest.fixture
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


@pytest.fixture(scope="session")
def ws() -> WorkspaceClient:
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
    return WorkspaceClient()
