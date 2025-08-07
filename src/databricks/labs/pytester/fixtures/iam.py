import logging
import warnings
from collections.abc import Callable, Generator, Iterable
from datetime import timedelta

import pytest
from pytest import fixture
from databricks.sdk.credentials_provider import OAuthCredentialsProvider, OauthCredentialsStrategy
from databricks.sdk.oauth import ClientCredentials, Token
from databricks.sdk.service.oauth2 import CreateServicePrincipalSecretResponse
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import StatementExecutionBackend, SqlBackend
from databricks.sdk import AccountGroupsAPI, GroupsAPI, WorkspaceClient, AccountClient
from databricks.sdk.config import Config
from databricks.sdk.errors import ResourceConflict, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import (
    User,
    Group,
    ServicePrincipal,
    Patch,
    PatchOp,
    ComplexValue,
    PatchSchema,
    WorkspacePermission,
)

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


class RunAs:
    def __init__(self, service_principal: ServicePrincipal, workspace_client: WorkspaceClient, env_or_skip):
        self._service_principal = service_principal
        self._workspace_client = workspace_client
        self._env_or_skip = env_or_skip

    @property
    def ws(self):
        return self._workspace_client

    @property
    def sql_backend(self) -> SqlBackend:
        # TODO: Switch to `__getattr__` + `SubRequest` to get a generic way of initializing all workspace fixtures.
        # This will allow us to remove the `sql_backend` fixture and make the `RunAs` class more generic.
        # It turns out to be more complicated than it first appears, because we don't get these at pytest.collect phase.
        warehouse_id = self._env_or_skip("DATABRICKS_WAREHOUSE_ID")
        return StatementExecutionBackend(self._workspace_client, warehouse_id)

    def sql_exec(self, statement: str) -> None:
        return self.sql_backend.execute(statement)

    def sql_fetch_all(self, statement: str) -> Iterable[Row]:
        return self.sql_backend.fetch(statement)

    def __getattr__(self, item: str):
        if item in self.__dict__:
            return self.__dict__[item]
        fixture_value = self._request.getfixturevalue(item)
        return fixture_value

    @property
    def display_name(self) -> str:
        assert self._service_principal.display_name is not None
        return self._service_principal.display_name

    @property
    def application_id(self) -> str:
        assert self._service_principal.application_id is not None
        return self._service_principal.application_id

    @property
    def id(self) -> str:
        assert self._service_principal.id is not None
        return self._service_principal.id

    def __repr__(self):
        return f'RunAs({self.display_name})'


def _make_workspace_client(
    ws: WorkspaceClient,
    created_secret: CreateServicePrincipalSecretResponse,
    service_principal: ServicePrincipal,
) -> WorkspaceClient:
    oidc = ws.config.oidc_endpoints
    assert oidc is not None, 'OIDC is required'
    application_id = service_principal.application_id
    secret_value = created_secret.secret
    assert application_id is not None
    assert secret_value is not None

    token_source = ClientCredentials(
        client_id=application_id,
        client_secret=secret_value,
        token_url=oidc.token_endpoint,
        scopes=["all-apis"],
        use_header=True,
    )

    def inner() -> dict[str, str]:
        inner_token = token_source.token()
        return {'Authorization': f'{inner_token.token_type} {inner_token.access_token}'}

    def token() -> Token:
        return token_source.token()

    credentials_provider = OAuthCredentialsProvider(inner, token)
    credentials_strategy = OauthCredentialsStrategy('oauth-m2m', lambda _: credentials_provider)
    ws_as_spn = WorkspaceClient(host=ws.config.host, credentials_strategy=credentials_strategy)
    return ws_as_spn


@fixture
def make_run_as(acc: AccountClient, ws: WorkspaceClient, make_random, env_or_skip, log_account_link, is_in_debug):
    """
    This fixture provides a function to create an account service principal via [`acc` fixture](#acc-fixture) and
    assign it to a workspace. The service principal is removed after the test is complete. The service principal is
    created with a random display name and assigned to the workspace with the default permissions.

    Use the `account_groups` argument to assign the service principal to account groups, which have the required
    permissions to perform a specific action.

    Example:

    ```python
    def test_run_as_lower_privilege_user(make_run_as, ws):
        run_as = make_run_as(account_groups=['account.group.name'])
        through_query = next(run_as.sql_fetch_all("SELECT CURRENT_USER() AS my_name"))
        me = ws.current_user.me()
        assert me.user_name != through_query.my_name
    ```

    Returned object has the following properties:
    * `ws`: Workspace client that is authenticated as the ephemeral service principal.
    * `sql_backend`: SQL backend that is authenticated as the ephemeral service principal.
    * `sql_exec`: Function to execute a SQL statement on behalf of the ephemeral service principal.
    * `sql_fetch_all`: Function to fetch all rows from a SQL statement on behalf of the ephemeral service principal.
    * `display_name`: Display name of the ephemeral service principal.
    * `application_id`: Application ID of the ephemeral service principal.
    * if you want to have other fixtures available in the context of the ephemeral service principal, you can override
      the [`ws` fixture](#ws-fixture) on the file level, which would make all workspace fixtures provided by this
      plugin to run as lower privilege ephemeral service principal. You cannot combine it with the account-admin-level
      principal you're using to create the ephemeral principal.

    Example:

    ```python
    from pytest import fixture

    @fixture
    def ws(make_run_as):
        run_as = make_run_as(account_groups=['account.group.used.for.all.tests.in.this.file'])
        return run_as.ws

    def test_creating_notebook_on_behalf_of_ephemeral_principal(make_notebook):
        notebook = make_notebook()
        assert notebook.exists()
    ```

    This fixture currently doesn't work with Databricks Metadata Service authentication on Azure Databricks.
    """

    if ws.config.auth_type == 'metadata-service' and ws.config.is_azure:
        # TODO: fix `invalid_scope: AADSTS1002012: The provided value for scope all-apis is not valid.` error
        #
        # We're having issues with the Azure Metadata Service and service principals. The error message is:
        # Client credential flows must have a scope value with /.default suffixed to the resource identifier
        # (application ID URI)
        pytest.skip('Azure Metadata Service does not support service principals')

    def create(*, account_groups: list[str] | None = None):
        workspace_id = ws.get_workspace_id()
        service_principal = acc.service_principals.create(display_name=f'spn-{make_random()}')
        assert service_principal.id is not None
        created_secret = acc.service_principal_secrets.create(service_principal.id)
        if account_groups:
            group_mapping = {}
            for group in acc.groups.list(attributes='id,displayName'):
                if group.id is None:
                    continue
                group_mapping[group.display_name] = group.id
            for group_name in account_groups:
                if group_name not in group_mapping:
                    raise ValueError(f'Group {group_name} does not exist')
                group_id = group_mapping[group_name]
                acc.groups.patch(
                    group_id,
                    operations=[
                        Patch(PatchOp.ADD, 'members', [ComplexValue(value=str(service_principal.id)).as_dict()]),
                    ],
                    schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                )
        permissions = [WorkspacePermission.USER]
        acc.workspace_assignment.update(workspace_id, int(service_principal.id), permissions=permissions)
        ws_as_spn = _make_workspace_client(ws, created_secret, service_principal)

        log_account_link('account service principal', f'users/serviceprincipals/{service_principal.id}')

        return RunAs(service_principal, ws_as_spn, env_or_skip)

    def remove(run_as: RunAs):
        service_principal_id = run_as._service_principal.id  # pylint: disable=protected-access
        assert service_principal_id is not None
        acc.service_principals.delete(service_principal_id)

    yield from factory("service principal", create, remove)
