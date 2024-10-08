import logging
import warnings
from collections.abc import Callable, Generator
from datetime import timedelta

from pytest import fixture
from databricks.sdk import AccountGroupsAPI, GroupsAPI, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import ResourceConflict, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import User, Group

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
    This fixture provides a function to manage Databricks workspace groups. Groups can be created with specified
    members and roles, and they will be deleted after the test is complete. Deals with eventual consistency issues by
    retrying the creation process for 30 seconds and then waiting for up to 3 minutes for the group to be provisioned.
    Returns an instance of `databricks.sdk.service.iam.Group`.

    Keyword arguments:
    * `members` (list of strings): A list of user IDs to add to the group.
    * `roles` (list of strings): A list of roles to assign to the group.
    * `display_name` (str): The display name of the group.
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


@fixture
def make_acc_group(acc, make_random, watchdog_purge_suffix):
    """
    This fixture provides a function to manage Databricks account groups. Groups can be created with
    specified members and roles, and they will be deleted after the test is complete.

    Has the same arguments and behavior as [`make_group` fixture](#make_group-fixture) but uses the account
    client instead of the workspace client.

    Example usage:
    ```python
    def test_new_account_group(make_acc_group, acc):
        group = make_acc_group()
        loaded = acc.groups.get(group.id)
        assert group.display_name == loaded.display_name
    ```
    """
    yield from _make_group("account group", acc.config, acc.groups, make_random, watchdog_purge_suffix)


def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


def _wait_group_provisioned(interface: AccountGroupsAPI | GroupsAPI, group: Group) -> None:
    """Wait for a group to be visible via the supplied group interface.

    Due to consistency issues in the group-management APIs, new groups are not always visible in a consistent manner
    after being created or modified. This method can be used to mitigate against this by checking that a group:

     - Is visible via the `.get()` interface;
     - Is visible via the `.list()` interface that enumerates groups.

    Visibility is assumed when 2 calls in a row return the expected results.

    Args:
          interface: the group-management interface to use for checking whether the groups are visible.
          group: the group whose visibility should be verified.
    Raises:
          NotFound: this is thrown if it takes longer than 90 seconds for the group to become visible via the
          management interface.
    """
    # Use double-checking to try and compensate for the lack of monotonic consistency with the group-management
    # interfaces: two subsequent calls need to succeed for us to proceed. (This is probabilistic, and not a guarantee.)
    # The REST API internals cache things for up to 60s, and we see times close to this during tests. The retry timeout
    # reflects this: if it's taking much longer then something else is wrong.
    group_id = group.id
    assert group_id is not None

    @retried(on=[NotFound], timeout=timedelta(seconds=90))
    def _double_get_group() -> None:
        interface.get(group_id)
        interface.get(group_id)

    def _check_group_in_listing() -> None:
        found_groups = interface.list(attributes="id", filter=f'id eq "{group_id}"')
        found_ids = {found_group.id for found_group in found_groups}
        if group_id not in found_ids:
            msg = f"Group id not (yet) found in group listing: {group_id}"
            raise NotFound(msg)

    @retried(on=[NotFound], timeout=timedelta(seconds=90))
    def _double_check_group_in_listing() -> None:
        _check_group_in_listing()
        _check_group_in_listing()

    _double_get_group()
    _double_check_group_in_listing()


def _make_group(
    name: str, cfg: Config, interface, make_random, watchdog_purge_suffix
) -> Generator[Callable[..., Group], None, None]:
    _not_specified = object()

    @retried(on=[ResourceConflict], timeout=timedelta(seconds=30))
    def create(
        *,
        members: list[str] | None = None,
        roles: list[str] | None = None,
        entitlements: list[str] | None = None,
        display_name: str | None = None,
        wait_for_provisioning: bool | object = _not_specified,
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
        if wait_for_provisioning is not _not_specified:
            warnings.warn(
                "Specifying wait_for_provisioning when making a group is deprecated; we always wait.",
                DeprecationWarning,
                # Call stack is: create()[iam.py], wrapper()[retries.py], inner()[baseline.py], client_code
                stacklevel=4,
            )
        # TODO: REQUEST_LIMIT_EXCEEDED: GetUserPermissionsRequest RPC token bucket limit has been exceeded.
        group = interface.create(**kwargs)
        if cfg.is_account_client:
            logger.info(f"Account group {group.display_name}: {cfg.host}/users/groups/{group.id}/members")
        else:
            logger.info(f"Workspace group {group.display_name}: {cfg.host}#setting/accounts/groups/{group.id}")

        _wait_group_provisioned(interface, group)

        return group

    yield from factory(name, create, lambda item: interface.delete(item.id))
