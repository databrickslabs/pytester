import logging

from databricks.sdk.service.iam import PermissionLevel

logger = logging.getLogger(__name__)


def test_notebook_permissions(make_notebook, make_notebook_permissions, make_group):
    group = make_group()
    notebook = make_notebook()
    acl = make_notebook_permissions(
        object_id=notebook,
        permission_level=PermissionLevel.CAN_RUN,
        group_name=group.display_name,  # noqa: F405
    )
    logger.info(f"created {acl}")
