import json

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute

from databricks.labs.pytester.fixtures.baseline import factory


@pytest.fixture
def make_cluster_policy(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks cluster policies.

    This fixture provides a function to manage Databricks cluster policies using the provided workspace (ws).
    Cluster policies can be created with a specified name and definition, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks cluster policies.

    Usage Example:
    --------------
    To manage Databricks cluster policies using the make_cluster_policy fixture:

    .. code-block:: python

        def test_cluster_policy_management(make_cluster_policy):
            policy_info = make_cluster_policy(name="my-policy")
            assert policy_info is not None
    """

    def create(*, name: str | None = None, **kwargs):
        if name is None:
            name = f"sdk-{make_random(4)}"
        if "definition" not in kwargs:
            kwargs["definition"] = json.dumps(
                {"spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": True}}
            )
        return ws.cluster_policies.create(name, **kwargs)  # type: ignore

    def cleanup_policy(policy_info):
        ws.cluster_policies.delete(policy_info.policy_id)

    return factory("cluster policy", create, cleanup_policy)


@pytest.fixture
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

    return factory("cluster", create, cleanup_cluster)


@pytest.fixture
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

    return factory("instance pool", create, cleanup_instance_pool)


@pytest.fixture
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

    return factory("job", create, cleanup_job)
