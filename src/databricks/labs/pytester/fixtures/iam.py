import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.pytester.fixtures.baseline import factory


@pytest.fixture
def make_user(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks workspace users.

    This fixture provides a function to manage Databricks workspace users using the provided workspace (ws).
    Users can be created with a generated user name, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks workspace users.

    Usage Example:
    --------------
    To manage Databricks workspace users using the make_user fixture:

    .. code-block:: python

        def test_user_management(make_user):
            user_info = make_user()
            assert user_info is not None
    """

    def create_user(**kwargs):
        return ws.users.create(user_name=f"sdk-{make_random(4)}@example.com".lower(), **kwargs)

    def cleanup_user(user_info):
        ws.users.delete(user_info.id)

    yield from factory("workspace user", create_user, cleanup_user)


def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


@pytest.fixture
def make_group(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks workspace groups.

    This fixture provides a function to manage Databricks workspace groups using the provided workspace (ws).
    Groups can be created with specified members and roles, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks workspace groups.

    Usage Example:
    --------------
    To manage Databricks workspace groups using the make_group fixture:

    .. code-block:: python

        def test_group_management(make_group):
            group_info = make_group(members=["user@example.com"], roles=["viewer"])
            assert group_info is not None
    """

    def create(
        *, members: list[str] | None = None, roles: list[str] | None = None, display_name: str | None = None, **kwargs
    ):
        kwargs["display_name"] = f"sdk-{make_random(4)}" if display_name is None else display_name
        if members is not None:
            kwargs["members"] = _scim_values(members)
        if roles is not None:
            kwargs["roles"] = _scim_values(roles)
        return ws.groups.create(**kwargs)

    def cleanup_group(group_info):
        ws.groups.delete(group_info.id)

    yield from factory("workspace group", create, cleanup_group)
