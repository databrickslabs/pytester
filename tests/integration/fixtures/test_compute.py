import logging
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.jobs import RunResultState, SparkPythonTask
from databricks.sdk.service.sql import EndpointTagPair

from databricks.labs.pytester.fixtures.watchdog import TEST_RESOURCE_PURGE_TIMEOUT

logger = logging.getLogger(__name__)


def test_cluster_policy(make_cluster_policy):
    logger.info(f"created {make_cluster_policy()}")


def test_cluster(make_cluster, env_or_skip):
    logger.info(f"created {make_cluster(single_node=True, instance_pool_id=env_or_skip('TEST_INSTANCE_POOL_ID'))}")


def test_instance_pool(make_instance_pool):
    logger.info(f"created {make_instance_pool()}")


def test_job(ws: WorkspaceClient, make_job, env_or_skip) -> None:
    job = make_job(instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"))
    run = ws.jobs.run_now(job.job_id)
    ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)
    run_state = ws.jobs.get_run(run_id=run.run_id).state
    assert run_state is not None and run_state.result_state == RunResultState.SUCCESS


def test_job_with_spark_python_task(ws: WorkspaceClient, make_job, env_or_skip) -> None:
    job = make_job(task_type=SparkPythonTask, instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"))
    run = ws.jobs.run_now(job.job_id)
    ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)
    run_state = ws.jobs.get_run(run_id=run.run_id).state
    assert run_state is not None and run_state.result_state == RunResultState.SUCCESS


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


def test_warehouse_has_custom_tag(ws, make_warehouse):
    new_warehouse = make_warehouse(tags=[EndpointTagPair(key="my-custom-tag", value="my-custom-tag-value")])
    created_warehouse = ws.warehouses.get(new_warehouse.response.id)
    warehouse_tags = created_warehouse.tags.as_dict()
    assert warehouse_tags["custom_tags"][1]["key"] == "my-custom-tag"


def test_remove_after_tag_jobs(ws, env_or_skip, make_job):
    new_job = make_job()
    created_job = ws.jobs.get(new_job.job_id)
    job_tags = created_job.settings.tags
    assert "RemoveAfter" in job_tags

    purge_time = datetime.strptime(job_tags["RemoveAfter"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_tag_clusters(ws, env_or_skip, make_cluster):
    new_cluster = make_cluster(single_node=True, instance_pool_id=env_or_skip('TEST_INSTANCE_POOL_ID'))
    created_cluster = ws.clusters.get(new_cluster.cluster_id)
    cluster_tags = created_cluster.custom_tags
    assert "RemoveAfter" in cluster_tags
    purge_time = datetime.strptime(cluster_tags["RemoveAfter"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_tag_warehouse(ws, env_or_skip, make_warehouse):
    new_warehouse = make_warehouse()
    created_warehouse = ws.warehouses.get(new_warehouse.response.id)
    warehouse_tags = created_warehouse.tags.as_dict()
    assert warehouse_tags["custom_tags"][0]["key"] == "RemoveAfter"
    remove_after_tag = warehouse_tags["custom_tags"][0]["value"]
    purge_time = datetime.strptime(remove_after_tag, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_tag_instance_pool(ws, make_instance_pool):
    new_instance_pool = make_instance_pool()
    created_instance_pool = ws.instance_pools.get(new_instance_pool.instance_pool_id)
    pool_tags = created_instance_pool.custom_tags
    assert "RemoveAfter" in pool_tags
    purge_time = datetime.strptime(pool_tags["RemoveAfter"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405
