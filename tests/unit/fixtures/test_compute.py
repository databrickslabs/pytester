from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, SparkPythonTask

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
    assert job.settings is not None
    assert job.settings.name == "dummy-jRANDOM"
    assert job.settings.tags == {"RemoveAfter": "2024091313"}
    tasks = job.settings.tasks
    assert isinstance(tasks, list) and len(tasks) == 1
    assert tasks[0].task_key == "RANDOM"
    assert tasks[0].description == "RANDOM"
    assert tasks[0].new_cluster is not None
    assert tasks[0].new_cluster.num_workers == 1
    assert tasks[0].new_cluster.spark_conf is None
    assert tasks[0].libraries is None
    assert tasks[0].timeout_seconds == 0
    environments = job.settings.environments
    assert environments is None


def test_make_job_with_name() -> None:
    _, job = call_stateful(make_job, name="test")
    assert job.settings is not None
    assert job.settings.name == "test"


def test_make_job_with_path() -> None:
    _, job = call_stateful(make_job, path="test.py")
    assert job.settings is not None
    tasks = job.settings.tasks
    assert isinstance(tasks, list) and len(tasks) == 1
    assert tasks[0].notebook_task is not None
    assert tasks[0].notebook_task.notebook_path == "test.py"


def test_make_job_with_content() -> None:
    ctx, job = call_stateful(make_job, content="print(2)")
    assert job.settings is not None
    tasks = job.settings.tasks
    assert isinstance(tasks, list) and len(tasks) == 1
    assert tasks[0].notebook_task is not None
    workspace_path = WorkspacePath(ctx["ws"], tasks[0].notebook_task.notebook_path)
    assert not workspace_path.suffix  # Notebooks have no suffix
    assert workspace_path.read_text() == "print(2)"


def test_make_job_with_spark_python_task() -> None:
    ctx, job = call_stateful(make_job, content="print(3)", task_type=SparkPythonTask)
    assert job.settings is not None
    tasks = job.settings.tasks
    assert isinstance(tasks, list) and len(tasks) == 1
    assert tasks[0].notebook_task is None
    assert tasks[0].spark_python_task is not None
    workspace_path = WorkspacePath(ctx["ws"], tasks[0].spark_python_task.python_file)
    assert workspace_path.suffix == ".py"  # Python files have suffix
    assert workspace_path.read_text() == "print(3)"


def test_make_job_with_instance_pool_id() -> None:
    _, job = call_stateful(make_job, instance_pool_id="test")
    assert job.settings is not None
    tasks = job.settings.tasks
    assert isinstance(tasks, list) and len(tasks) == 1
    assert tasks[0].new_cluster is not None
    assert tasks[0].new_cluster.instance_pool_id == "test"


def test_make_job_with_spark_conf() -> None:
    _, job = call_stateful(make_job, spark_conf={"value": "test"})
    assert job.settings is not None
    tasks = job.settings.tasks
    assert isinstance(tasks, list) and len(tasks) == 1
    assert tasks[0].new_cluster is not None
    assert tasks[0].new_cluster.spark_conf == {"value": "test"}


def test_make_job_with_tags() -> None:
    _, job = call_stateful(make_job, tags={"value": "test"})
    assert job.settings is not None
    assert job.settings.tags == {"value": "test", "RemoveAfter": "2024091313"}


def test_make_job_with_tasks() -> None:
    _, job = call_stateful(make_job, tasks=["CustomTasks"])
    assert job.settings is not None
    assert job.settings.tasks == ["CustomTasks"]


def test_make_job_with_environment() -> None:
    environment = Environment(environment_version="4")
    job_environment = JobEnvironment(environment_key="job_environment", spec=environment)
    _, job = call_stateful(make_job, environments=[job_environment])
    assert job.settings.environments is not None
    assert job.settings.environments[0] == job_environment


def test_make_pipeline_no_args() -> None:
    ctx, pipeline = call_stateful(make_pipeline)
    assert ctx is not None
    assert pipeline is not None


def test_make_warehouse_no_args() -> None:
    ctx, warehouse = call_stateful(make_warehouse)
    assert ctx is not None
    assert warehouse is not None
