import logging
from collections.abc import Generator
from datetime import timedelta

from pytest import fixture
from databricks.sdk.config import Config
from databricks.sdk.errors import ResourceConflict, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import User, Group
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.pytester.fixtures.baseline import factory

logger = logging.getLogger(__name__)


@fixture
def make_user(ws, make_random, log_workspace_link, watchdog_purge_suffix):
    """
    This fixture returns a function that creates a Databricks workspace user
    and removes it after the test is complete. In case of random naming conflicts,
    the fixture will retry the creation process for 30 seconds. Returns an instance
    of `databricks.sdk.service.iam.User`. Usage:

    ```python
    def test_new_user(make_user, ws):
        new_user = make_user()
        home_dir = ws.workspace.get_status(f"/Users/{new_user.user_name}")
        assert home_dir.object_type == ObjectType.DIRECTORY
    ```
    """

    @retried(on=[ResourceConflict], timeout=timedelta(seconds=30))
    def create(**kwargs) -> User:
        user_name = f"dummy-{make_random(4)}-{watchdog_purge_suffix}@example.com".lower()
        user = ws.users.create(user_name=user_name, **kwargs)
        log_workspace_link(user.user_name, f'settings/workspace/identity-and-access/users/{user.id}')
        return user

    yield from factory("workspace user", create, lambda item: ws.users.delete(item.id))


@fixture
def make_group(ws: WorkspaceClient, make_random, watchdog_purge_suffix):
    """
    This fixture provides a function to manage Databricks workspace groups. Groups can be created with
    specified members and roles, and they will be deleted after the test is complete. Deals with eventual
    consistency issues by retrying the creation process for 30 seconds and allowing up to two minutes
    for group to be provisioned. Returns an instance of `databricks.sdk.service.iam.Group`.

    Keyword arguments:
    * `members` (list of strings): A list of user IDs to add to the group.
    * `roles` (list of strings): A list of roles to assign to the group.
    * `display_name` (str): The display name of the group.
    * `wait_for_provisioning` (bool): If `True`, the function will wait for the group to be provisioned.
    * `entitlements` (list of strings): A list of entitlements to assign to the group.

    The following example creates a group with a single member and independently verifies that the group was created:

    ```python
    def test_new_group(make_group, make_user, ws):
        user = make_user()
        group = make_group(members=[user.id])
        loaded = ws.groups.get(group.id)
        assert group.display_name == loaded.display_name
        assert group.members == loaded.members
    ```
    """
    yield from _make_group("workspace group", ws.config, ws.groups, make_random, watchdog_purge_suffix)


def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


def _make_group(name: str, cfg: Config, interface, make_random, watchdog_purge_suffix) -> Generator[Group, None, None]:
    @retried(on=[ResourceConflict], timeout=timedelta(seconds=30))
    def create(
        *,
        members: list[str] | None = None,
        roles: list[str] | None = None,
        entitlements: list[str] | None = None,
        display_name: str | None = None,
        wait_for_provisioning: bool = False,
        **kwargs,
    ):
        kwargs["display_name"] = (
            f"sdk-{make_random(4)}-{watchdog_purge_suffix}" if display_name is None else display_name
        )
        if members is not None:
            kwargs["members"] = _scim_values(members)
        if roles is not None:
            kwargs["roles"] = _scim_values(roles)
        if entitlements is not None:
            kwargs["entitlements"] = _scim_values(entitlements)
        # TODO: REQUEST_LIMIT_EXCEEDED: GetUserPermissionsRequest RPC token bucket limit has been exceeded.
        group = interface.create(**kwargs)
        if cfg.is_account_client:
            logger.info(f"Account group {group.display_name}: {cfg.host}/users/groups/{group.id}/members")
        else:
            logger.info(f"Workspace group {group.display_name}: {cfg.host}#setting/accounts/groups/{group.id}")

        @retried(on=[NotFound], timeout=timedelta(minutes=2))
        def _wait_for_provisioning() -> None:
            interface.get(group.id)

        if wait_for_provisioning:
            _wait_for_provisioning()

        return group

    yield from factory(name, create, lambda item: interface.delete(item.id))
