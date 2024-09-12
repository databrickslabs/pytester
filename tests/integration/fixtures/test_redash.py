from databricks.sdk.service.sql import PermissionLevel


def test_permissions_for_redash(
    make_user,
    make_query,
    make_query_permissions,
):
    user = make_user()
    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=PermissionLevel.CAN_EDIT,
        user_name=user.display_name,
    )
