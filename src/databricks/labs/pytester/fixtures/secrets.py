from pytest import fixture
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from databricks.labs.pytester.fixtures.baseline import factory


@fixture
def make_secret_scope(ws: WorkspaceClient, make_random):
    """
    This fixture provides a function to create secret scopes. The created secret scope will be
    deleted after the test is complete. Returns the name of the secret scope.

    To create a secret scope and use it within a test function:

    ```python
    def test_secret_scope_creation(make_secret_scope):
        secret_scope_name = make_secret_scope()
        assert secret_scope_name.startswith("dummy-")
    ```
    """

    def create(**kwargs):
        name = f"dummy-{make_random(4)}"
        ws.secrets.create_scope(name, **kwargs)
        return name

    yield from factory("secret scope", create, ws.secrets.delete_scope)


@fixture
def make_secret_scope_acl(ws: WorkspaceClient):
    """
    This fixture provides a function to manage access control lists (ACLs) for secret scopes.
    ACLs define permissions for principals (users or groups) on specific secret scopes.

    Arguments:
    - `scope`: The name of the secret scope.
    - `principal`: The name of the principal (user or group).
    - `permission`: The permission level for the principal on the secret scope.

    Returns a tuple containing the secret scope name and the principal name.

    To manage secret scope ACLs using the make_secret_scope_acl fixture:

    ```python
    from databricks.sdk.service.workspace import AclPermission

    def test_secret_scope_acl_management(make_user, make_secret_scope, make_secret_scope_acl):
        scope_name = make_secret_scope()
        principal_name = make_user().display_name
        permission = AclPermission.READ

        acl_info = make_secret_scope_acl(
            scope=scope_name,
            principal=principal_name,
            permission=permission,
        )
        assert acl_info == (scope_name, principal_name)
    ```
    """

    def create(*, scope: str, principal: str, permission: workspace.AclPermission):
        ws.secrets.put_acl(scope, principal, permission)
        return scope, principal

    def cleanup(acl_info):
        scope, principal = acl_info
        ws.secrets.delete_acl(scope, principal)

    yield from factory("secret scope acl", create, cleanup)
