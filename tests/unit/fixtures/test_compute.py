from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk.service.jobs import Task, SparkPythonTask

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


def test_make_job_no_args() -> None:
    ctx, job = call_stateful(make_job)
    assert ctx is not None
    assert job is not None
    assert job.settings.name == "dummy-jRANDOM"
    assert job.settings.tags == [{"key": "RemoveAfter", "value": "2024091313"}]
    assert len(job.settings.tasks) == 1
    assert job.settings.tasks[0].task_key == "RANDOM"
    assert job.settings.tasks[0].description == "RANDOM"
    assert job.settings.tasks[0].new_cluster.num_workers == 1
    assert job.settings.tasks[0].new_cluster.spark_conf is None
    assert job.settings.tasks[0].libraries is None
    assert job.settings.tasks[0].timeout_seconds == 0


def test_make_job_with_name() -> None:
    ctx, job = call_stateful(make_job, name="test")
    assert job.settings.name == "test"


def test_make_job_with_path() -> None:
    ctx, job = call_stateful(make_job, path="test.py")
    tasks: list[Task] = job.settings.tasks
    assert len(tasks) == 1
    assert tasks[0].notebook_task.notebook_path == "test.py"


def test_make_job_with_content() -> None:
    ctx, job = call_stateful(make_job, content="print(2)")
    tasks = job.settings.tasks
    assert len(tasks) == 1
    workspace_path = WorkspacePath(ctx["ws"], tasks[0].notebook_task.notebook_path)
    assert workspace_path.read_text() == "print(2)"


def test_make_job_with_spark_python_task() -> None:
    ctx, job = call_stateful(make_job, path="test.py", task_type=SparkPythonTask)
    tasks = job.settings.tasks
    assert len(tasks) == 1
    assert tasks[0].notebook_task is None
    assert tasks[0].spark_python_task is not None
    assert tasks[0].spark_python_task.python_file == "test.py"


def test_make_job_with_spark_conf() -> None:
    _, job = call_stateful(make_job, spark_conf={"value": "test"})
    tasks = job.settings.tasks
    assert len(tasks) == 1
    assert tasks[0].new_cluster.spark_conf == {"value": "test"}


def test_make_pipeline_no_args():
    ctx, pipeline = call_stateful(make_pipeline)
    assert ctx is not None
    assert pipeline is not None


def test_make_warehouse_no_args():
    ctx, warehouse = call_stateful(make_warehouse)
    assert ctx is not None
    assert warehouse is not None
