import pytest
from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service import sql, iam
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.sql import GetResponse

from databricks.labs.pytester.fixtures.baseline import factory

# pylint: disable=too-complex


class _PermissionsChange:
    def __init__(self, object_id: str, before: list[iam.AccessControlRequest], after: list[iam.AccessControlRequest]):
        self.object_id = object_id
        self.before = before
        self.after = after

    @staticmethod
    def _principal(acr: iam.AccessControlRequest) -> str:
        if acr.user_name is not None:
            return f"user_name {acr.user_name}"
        if acr.group_name is not None:
            return f"group_name {acr.group_name}"
        return f"service_principal_name {acr.service_principal_name}"

    def _list(self, acl: list[iam.AccessControlRequest]):
        return ", ".join(f"{self._principal(_)} {_.permission_level.value}" for _ in acl)

    def __repr__(self):
        return f"{self.object_id} [{self._list(self.before)}] -> [{self._list(self.after)}]"


class _RedashPermissionsChange:
    def __init__(self, object_id: str, before: list[sql.AccessControl], after: list[sql.AccessControl]):
        self.object_id = object_id
        self.before = before
        self.after = after

    @staticmethod
    def _principal(acr: sql.AccessControl) -> str:
        if acr.user_name is not None:
            return f"user_name {acr.user_name}"
        return f"group_name {acr.group_name}"

    def _list(self, acl: list[sql.AccessControl]):
        return ", ".join(f"{self._principal(_)} {_.permission_level.value}" for _ in acl)

    def __repr__(self):
        return f"{self.object_id} [{self._list(self.before)}] -> [{self._list(self.after)}]"


def _make_permissions_factory(name, resource_type, levels, id_retriever):
    def _non_inherited(acl: iam.ObjectPermissions):
        out: list[iam.AccessControlRequest] = []
        assert acl.access_control_list is not None
        for access_control in acl.access_control_list:
            if not access_control.all_permissions:
                continue
            for permission in access_control.all_permissions:
                if not permission.inherited:
                    continue
                out.append(
                    iam.AccessControlRequest(
                        permission_level=permission.permission_level,
                        group_name=access_control.group_name,
                        user_name=access_control.user_name,
                        service_principal_name=access_control.service_principal_name,
                    )
                )
        return out

    def _make_permissions(ws):
        def create(
            *,
            object_id: str,
            permission_level: iam.PermissionLevel | None = None,
            group_name: str | None = None,
            user_name: str | None = None,
            service_principal_name: str | None = None,
            access_control_list: list[iam.AccessControlRequest] | None = None,
        ):
            nothing_specified = permission_level is None and access_control_list is None
            both_specified = permission_level is not None and access_control_list is not None
            if nothing_specified or both_specified:
                msg = "either permission_level or access_control_list has to be specified"
                raise ValueError(msg)

            object_id = id_retriever(ws, object_id)
            initial = _non_inherited(ws.permissions.get(resource_type, object_id))
            if access_control_list is None:
                if permission_level not in levels:
                    assert permission_level is not None
                    names = ", ".join(_.value for _ in levels)
                    msg = f"invalid permission level: {permission_level.value}. Valid levels: {names}"
                    raise ValueError(msg)

                access_control_list = []
                if group_name is not None:
                    access_control_list.append(
                        iam.AccessControlRequest(
                            group_name=group_name,
                            permission_level=permission_level,
                        )
                    )
                if user_name is not None:
                    access_control_list.append(
                        iam.AccessControlRequest(
                            user_name=user_name,
                            permission_level=permission_level,
                        )
                    )
                if service_principal_name is not None:
                    access_control_list.append(
                        iam.AccessControlRequest(
                            service_principal_name=service_principal_name,
                            permission_level=permission_level,
                        )
                    )
            ws.permissions.update(resource_type, object_id, access_control_list=access_control_list)
            return _PermissionsChange(object_id, initial, access_control_list)

        def remove(change: _PermissionsChange):
            try:
                ws.permissions.set(resource_type, change.object_id, access_control_list=change.before)
            except InvalidParameterValue:
                pass

        yield from factory(f"{name} permissions", create, remove)

    return _make_permissions


def _make_redash_permissions_factory(name, resource_type, levels, id_retriever):
    def _non_inherited(acl: GetResponse):
        out: list[sql.AccessControl] = []
        assert acl.access_control_list is not None
        for access_control in acl.access_control_list:
            out.append(
                sql.AccessControl(
                    permission_level=access_control.permission_level,
                    group_name=access_control.group_name,
                    user_name=access_control.user_name,
                )
            )
        return out

    def _make_permissions(ws):
        def create(
            *,
            object_id: str,
            permission_level: sql.PermissionLevel | None = None,
            group_name: str | None = None,
            user_name: str | None = None,
            access_control_list: list[sql.AccessControl] | None = None,
        ):
            nothing_specified = permission_level is None and access_control_list is None
            both_specified = permission_level is not None and access_control_list is not None
            if nothing_specified or both_specified:
                msg = "either permission_level or access_control_list has to be specified"
                raise ValueError(msg)

            object_id = id_retriever(ws, object_id)
            initial = _non_inherited(ws.dbsql_permissions.get(resource_type, object_id))

            if access_control_list is None:
                if permission_level not in levels:
                    assert permission_level is not None
                    names = ", ".join(_.value for _ in levels)
                    msg = f"invalid permission level: {permission_level.value}. Valid levels: {names}"
                    raise ValueError(msg)

                access_control_list = []
                if group_name is not None:
                    access_control_list.append(
                        sql.AccessControl(
                            group_name=group_name,
                            permission_level=permission_level,
                        )
                    )
                if user_name is not None:
                    access_control_list.append(
                        sql.AccessControl(
                            user_name=user_name,
                            permission_level=permission_level,
                        )
                    )

            ws.dbsql_permissions.set(resource_type, object_id, access_control_list=access_control_list)
            return _RedashPermissionsChange(object_id, initial, access_control_list)

        def remove(change: _RedashPermissionsChange):
            ws.dbsql_permissions.set(
                sql.ObjectTypePlural(resource_type), change.object_id, access_control_list=change.before
            )

        yield from factory(f"{name} permissions", create, remove)

    return _make_permissions


def _simple(_, object_id):
    return object_id


def _path(ws, path):
    return ws.workspace.get_status(path).object_id


make_cluster_policy_permissions = pytest.fixture(
    _make_permissions_factory(
        "cluster_policy",
        "cluster-policies",
        [
            PermissionLevel.CAN_USE,
        ],
        _simple,
    )
)

make_instance_pool_permissions = pytest.fixture(
    _make_permissions_factory(
        "instance_pool",
        "instance-pools",
        [
            PermissionLevel.CAN_ATTACH_TO,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
make_cluster_permissions = pytest.fixture(
    _make_permissions_factory(
        "cluster",
        "clusters",
        [
            PermissionLevel.CAN_ATTACH_TO,
            PermissionLevel.CAN_RESTART,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
make_pipeline_permissions = pytest.fixture(
    _make_permissions_factory(
        "pipeline",
        "pipelines",
        [
            PermissionLevel.CAN_VIEW,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.IS_OWNER,  # cannot be a group
        ],
        _simple,
    )
)
make_job_permissions = pytest.fixture(
    _make_permissions_factory(
        "job",
        "jobs",
        [
            PermissionLevel.CAN_VIEW,
            PermissionLevel.CAN_MANAGE_RUN,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.IS_OWNER,  # cannot be a group
        ],
        _simple,
    )
)
make_notebook_permissions = pytest.fixture(
    _make_permissions_factory(
        "notebook",
        "notebooks",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE,
        ],
        _path,
    )
)
make_directory_permissions = pytest.fixture(
    _make_permissions_factory(
        "directory",
        "directories",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE,
        ],
        _path,
    )
)
make_workspace_file_permissions = pytest.fixture(
    _make_permissions_factory(
        "workspace_file",
        "files",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
make_workspace_file_path_permissions = pytest.fixture(
    _make_permissions_factory(
        "workspace_file_path",
        "files",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE,
        ],
        _path,
    )
)
make_repo_permissions = pytest.fixture(
    _make_permissions_factory(
        "repo",
        "repos",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE,
        ],
        _path,
    )
)
make_authorization_permissions = pytest.fixture(
    _make_permissions_factory(
        "authorization",
        "authorization",
        [
            PermissionLevel.CAN_USE,
        ],
        _simple,
    )
)
make_warehouse_permissions = pytest.fixture(
    _make_permissions_factory(
        "warehouse",
        "sql/warehouses",
        [
            PermissionLevel.CAN_USE,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
make_lakeview_dashboard_permissions = pytest.fixture(
    _make_permissions_factory(
        "lakeview_dashboard",
        "dashboards",
        # The `CAN_READ` permission is consistent with the documentation (see below),
        # but not with the databricks UI as it shows `CAN_VIEW` instead.
        # https://docs.databricks.com/en/dashboards/tutorials/manage-permissions.html#get-workspace-object-permission-levels
        [
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.CAN_READ,
        ],
        _simple,
    )
)
make_dashboard_permissions = pytest.fixture(
    _make_redash_permissions_factory(
        "dashboard",
        "dashboards",
        [
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.CAN_VIEW,
        ],
        _simple,
    )
)
make_alert_permissions = pytest.fixture(
    _make_redash_permissions_factory(
        "alert",
        "alerts",
        [
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.CAN_VIEW,
        ],
        _simple,
    )
)
make_query_permissions = pytest.fixture(
    _make_redash_permissions_factory(
        "query",
        "queries",
        [
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_RUN,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.CAN_VIEW,
        ],
        _simple,
    )
)
make_experiment_permissions = pytest.fixture(
    _make_permissions_factory(
        "experiment",
        "experiments",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
make_registered_model_permissions = pytest.fixture(
    _make_permissions_factory(
        "registered_model",
        "registered-models",
        [
            PermissionLevel.CAN_READ,
            PermissionLevel.CAN_EDIT,
            PermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
            PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
make_serving_endpoint_permissions = pytest.fixture(
    _make_permissions_factory(
        "serving_endpoint",
        "serving-endpoints",
        [
            PermissionLevel.CAN_VIEW,
            PermissionLevel.CAN_MANAGE,
            PermissionLevel.CAN_QUERY,
        ],
        _simple,
    )
)
make_feature_table_permissions = pytest.fixture(
    _make_permissions_factory(
        "feature_table",
        "feature-tables",
        [
            PermissionLevel.CAN_VIEW_METADATA,
            PermissionLevel.CAN_EDIT_METADATA,
            PermissionLevel.CAN_MANAGE,
        ],
        _simple,
    )
)
