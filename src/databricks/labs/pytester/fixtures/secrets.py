import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from databricks.labs.pytester.fixtures.baseline import factory


@pytest.fixture
def make_secret_scope(ws: WorkspaceClient, make_random):
    """
    Fixture to create secret scopes.

    This fixture provides a function to create secret scopes using the provided workspace (ws)
    and the make_random function for generating unique names. The created secret scope will be
    deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to create secret scopes.

    Usage Example:
    --------------
    To create a secret scope and use it within a test function:

    .. code-block:: python

        def test_secret_scope_creation(make_secret_scope):
            secret_scope_name = make_secret_scope()
            assert secret_scope_name.startswith("sdk-")
    """

    def create(**kwargs):
        name = f"sdk-{make_random(4)}"
        ws.secrets.create_scope(name, **kwargs)
        return name

    yield from factory("secret scope", create, ws.secrets.delete_scope)


@pytest.fixture
def make_secret_scope_acl(ws: WorkspaceClient):
    """
    Fixture to manage secret scope access control lists (ACLs).

    This fixture provides a function to manage access control lists (ACLs) for secret scopes
    using the provided workspace (ws). ACLs define permissions for principals (users or groups)
    on specific secret scopes.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.

    Returns:
    --------
    function:
        A function to manage secret scope ACLs.

    Usage Example:
    --------------
    To manage secret scope ACLs using the make_secret_scope_acl fixture:

    .. code-block:: python

        def test_secret_scope_acl_management(make_secret_scope_acl):
            scope_name = "my_secret_scope"
            principal_name = "user@example.com"
            permission = workspace.AclPermission.READ

            acl_info = make_secret_scope_acl(scope=scope_name, principal=principal_name, permission=permission)
            assert acl_info == (scope_name, principal_name)
    """

    def create(*, scope: str, principal: str, permission: workspace.AclPermission):
        ws.secrets.put_acl(scope, principal, permission)
        return scope, principal

    def cleanup(acl_info):
        scope, principal = acl_info
        ws.secrets.delete_acl(scope, principal)

    return factory("secret scope acl", create, cleanup)
