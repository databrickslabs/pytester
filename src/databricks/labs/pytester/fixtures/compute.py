import json
from collections.abc import Generator

from pytest import fixture
from databricks.sdk.service.compute import CreatePolicyResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute

from databricks.labs.pytester.fixtures.baseline import factory, get_purge_suffix


@fixture
def make_cluster_policy(ws, make_random, log_workspace_link) -> Generator[CreatePolicyResponse, None, None]:
    """
    Create a Databricks cluster policy and clean it up after the test. Returns a function to create cluster policies,
    which returns `databricks.sdk.service.compute.CreatePolicyResponse` instance.

    Keyword Arguments:
    * `name` (str, optional): The name of the cluster policy. If not provided, a random name will be generated.

    Usage:
    ```python
    def test_cluster_policy(make_cluster_policy):
        logger.info(f"created {make_cluster_policy()}")
    ```
    """

    def create(*, name: str | None = None, **kwargs) -> CreatePolicyResponse:
        if name is None:
            name = f"dummy-{make_random(4)}-{get_purge_suffix()}"
        if "definition" not in kwargs:
            kwargs["definition"] = json.dumps(
                {
                    "spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": "true"},
                }
            )
        cluster_policy = ws.cluster_policies.create(name=name, **kwargs)
        log_workspace_link(name, f'setting/clusters/cluster-policies/view/{cluster_policy.policy_id}')
        return cluster_policy

    yield from factory("cluster policy", create, lambda item: ws.cluster_policies.delete(item.policy_id))


@fixture
def make_cluster(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks clusters.

    This fixture provides a function to manage Databricks clusters using the provided workspace (ws).
    Clusters can be created with specified configurations, and they will be permanently deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks clusters.

    Usage Example:
    --------------
    To manage Databricks clusters using the make_cluster fixture:

    .. code-block:: python

        def test_cluster_management(make_cluster):
            cluster_info = make_cluster(cluster_name="my-cluster", single_node=True)
            assert cluster_info is not None
    """

    def create(
        *,
        single_node: bool = False,
        cluster_name: str | None = None,
        spark_version: str | None = None,
        autotermination_minutes=10,
        **kwargs,
    ):
        if cluster_name is None:
            cluster_name = f"sdk-{make_random(4)}"
        if spark_version is None:
            spark_version = ws.clusters.select_spark_version(latest=True)
        if single_node:
            kwargs["num_workers"] = 0
            kwargs["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
            kwargs["custom_tags"] = {"ResourceClass": "SingleNode"}
        elif "instance_pool_id" not in kwargs:
            kwargs["node_type_id"] = ws.clusters.select_node_type(local_disk=True)

        return ws.clusters.create(
            cluster_name=cluster_name,
            spark_version=spark_version,
            autotermination_minutes=autotermination_minutes,
            **kwargs,
        )

    def cleanup_cluster(cluster_info):
        ws.clusters.permanent_delete(cluster_info.cluster_id)

    yield from factory("cluster", create, cleanup_cluster)


@fixture
def make_instance_pool(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks instance pools.

    This fixture provides a function to manage Databricks instance pools using the provided workspace (ws).
    Instance pools can be created with specified configurations, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks instance pools.

    Usage Example:
    --------------
    To manage Databricks instance pools using the make_instance_pool fixture:

    .. code-block:: python

        def test_instance_pool_management(make_instance_pool):
            instance_pool_info = make_instance_pool(instance_pool_name="my-pool")
            assert instance_pool_info is not None
    """

    def create(*, instance_pool_name=None, node_type_id=None, **kwargs):
        if instance_pool_name is None:
            instance_pool_name = f"sdk-{make_random(4)}"
        if node_type_id is None:
            node_type_id = ws.clusters.select_node_type(local_disk=True)
        return ws.instance_pools.create(instance_pool_name, node_type_id, **kwargs)

    def cleanup_instance_pool(instance_pool_info):
        ws.instance_pools.delete(instance_pool_info.instance_pool_id)

    yield from factory("instance pool", create, cleanup_instance_pool)


@fixture
def make_job(ws: WorkspaceClient, make_random, make_notebook):
    """
    Fixture to manage Databricks jobs.

    This fixture provides a function to manage Databricks jobs using the provided workspace (ws).
    Jobs can be created with specified configurations, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.
    make_notebook : function
        The make_notebook fixture to create a notebook path.

    Returns:
    --------
    function:
        A function to manage Databricks jobs.

    Usage Example:
    --------------
    To manage Databricks jobs using the make_job fixture:

    .. code-block:: python

        def test_job_management(make_job):
            job_info = make_job(name="my-job")
            assert job_info is not None
    """

    def create(**kwargs):
        if "name" not in kwargs:
            kwargs["name"] = f"sdk-{make_random(4)}"
        if "tasks" not in kwargs:
            kwargs["tasks"] = [
                jobs.Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=compute.ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                    ),
                    notebook_task=jobs.NotebookTask(notebook_path=make_notebook()),
                    timeout_seconds=0,
                )
            ]
        return ws.jobs.create(**kwargs)

    def cleanup_job(job_info):
        ws.jobs.delete(job_info.job_id)

    yield from factory("job", create, cleanup_job)
