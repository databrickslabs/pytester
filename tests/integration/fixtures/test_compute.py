import logging

from databricks.sdk.service.iam import PermissionLevel

logger = logging.getLogger(__name__)


def test_cluster_policy(make_cluster_policy):
    logger.info(f"created {make_cluster_policy()}")


def test_cluster(make_cluster, env_or_skip):
    logger.info(f"created {make_cluster(single_node=True, instance_pool_id=env_or_skip('TEST_INSTANCE_POOL_ID'))}")


def test_instance_pool(make_instance_pool):
    logger.info(f"created {make_instance_pool()}")


def test_job(make_job):
    logger.info(f"created {make_job()}")


def test_pipeline(make_pipeline, make_pipeline_permissions, make_group):
    group = make_group()
    pipeline = make_pipeline()
    make_pipeline_permissions(
        object_id=pipeline.pipeline_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group.display_name,
    )


def test_warehouse_has_remove_after_tag(ws, make_warehouse):
    new_warehouse = make_warehouse()
    created_warehouse = ws.warehouses.get(new_warehouse.response.id)
    warehouse_tags = created_warehouse.tags.as_dict()
    assert warehouse_tags["custom_tags"][0]["key"] == "RemoveAfter"
