from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.pytester.fixtures.permissions import make_cluster_permissions, make_query_permissions
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_cluster_permissions_no_args():
    ctx, cluster_permissions = call_stateful(
        make_cluster_permissions,
        object_id="dummy",
        permission_level=PermissionLevel.CAN_MANAGE,
    )
    assert ctx is not None
    assert cluster_permissions is not None


def test_make_query_permissions_no_args():
    ctx, query_permissions = call_stateful(
        make_query_permissions,
        object_id="dummy",
        permission_level=PermissionLevel.CAN_MANAGE,
    )
    assert ctx is not None
    assert query_permissions is not None
