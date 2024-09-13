from databricks.labs.pytester.fixtures.compute import (
    make_cluster_policy,
    make_cluster,
    make_instance_pool,
    make_job,
    make_pipeline,
    make_warehouse,
)
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_cluster_policy_no_args():
    ctx, cluster_policy = call_stateful(make_cluster_policy)
    assert ctx is not None
    assert cluster_policy is not None


def test_make_cluster_no_args():
    ctx, cluster = call_stateful(make_cluster)
    assert ctx is not None
    assert cluster is not None


def test_make_instance_pool_no_args():
    ctx, instance_pool = call_stateful(make_instance_pool)
    assert ctx is not None
    assert instance_pool is not None


def test_make_job_no_args():
    ctx, job = call_stateful(make_job)
    assert ctx is not None
    assert job is not None


def test_make_pipeline_no_args():
    ctx, pipeline = call_stateful(make_pipeline)
    assert ctx is not None
    assert pipeline is not None


def test_make_warehouse_no_args():
    ctx, warehouse = call_stateful(make_warehouse)
    assert ctx is not None
    assert warehouse is not None
