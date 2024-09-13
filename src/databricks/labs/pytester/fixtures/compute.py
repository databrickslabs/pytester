import json
from collections.abc import Generator
from pathlib import Path
from pytest import fixture

from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    EndpointTags,
    EndpointTagPair,
    GetWarehouseResponse,
)

from databricks.sdk.service.pipelines import CreatePipelineResponse, PipelineLibrary, NotebookLibrary, PipelineCluster
from databricks.sdk.service.jobs import Job, NotebookTask, Task
from databricks.sdk.service._internal import Wait
from databricks.sdk.service.compute import CreatePolicyResponse, ClusterDetails, ClusterSpec, CreateInstancePoolResponse

from databricks.labs.pytester.fixtures.baseline import factory


@fixture
def make_cluster_policy(
    ws,
    make_random,
    log_workspace_link,
    watchdog_purge_suffix,
) -> Generator[CreatePolicyResponse, None, None]:
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
            name = f"dummy-{make_random(4)}-{watchdog_purge_suffix}"
        if "definition" not in kwargs:
            kwargs["definition"] = json.dumps(
                {
                    "spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": "true"},
                }
            )
        cluster_policy = ws.cluster_policies.create(name=name, **kwargs)
        log_workspace_link(name, f'setting/clusters/cluster-policies/view/{cluster_policy.policy_id}', anchor=False)
        return cluster_policy

    yield from factory("cluster policy", create, lambda item: ws.cluster_policies.delete(item.policy_id))


@fixture
def make_cluster(ws, make_random, log_workspace_link, watchdog_remove_after) -> Generator[ClusterDetails, None, None]:
    """
    Create a Databricks cluster, waits for it to start, and clean it up after the test.
    Returns a function to create clusters. You can get `cluster_id` attribute from the returned object.

    Keyword Arguments:
    * `single_node` (bool, optional): Whether to create a single-node cluster. Defaults to False.
    * `cluster_name` (str, optional): The name of the cluster. If not provided, a random name will be generated.
    * `spark_version` (str, optional): The Spark version of the cluster. If not provided, the latest version will be used.
    * `autotermination_minutes` (int, optional): The number of minutes before the cluster is automatically terminated. Defaults to 10.

    Usage:
    ```python
    def test_cluster(make_cluster):
        logger.info(f"created {make_cluster(single_node=True)}")
    ```
    """

    def create(
        *,
        single_node: bool = False,
        cluster_name: str | None = None,
        spark_version: str | None = None,
        autotermination_minutes=10,
        **kwargs,
    ) -> Wait[ClusterDetails]:
        if cluster_name is None:
            cluster_name = f"dummy-{make_random(4)}"
        if spark_version is None:
            spark_version = ws.clusters.select_spark_version(latest=True)
        if single_node:
            kwargs["num_workers"] = 0
            if "spark_conf" in kwargs:
                kwargs["spark_conf"] = kwargs["spark_conf"] | {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": "local[*]",
                }
            else:
                kwargs["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
            kwargs["custom_tags"] = {"ResourceClass": "SingleNode"}
        if "instance_pool_id" not in kwargs:
            kwargs["node_type_id"] = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
        if "custom_tags" not in kwargs:
            kwargs["custom_tags"] = {"RemoveAfter": watchdog_remove_after}
        else:
            kwargs["custom_tags"]["RemoveAfter"] = watchdog_remove_after
        wait = ws.clusters.create(
            cluster_name=cluster_name,
            spark_version=spark_version,
            autotermination_minutes=autotermination_minutes,
            **kwargs,
        )
        log_workspace_link(cluster_name, f'compute/clusters/{wait.cluster_id}', anchor=False)
        return wait

    yield from factory("cluster", create, lambda item: ws.clusters.permanent_delete(item.cluster_id))


@fixture
def make_instance_pool(
    ws,
    make_random,
    log_workspace_link,
    watchdog_remove_after,
) -> Generator[CreateInstancePoolResponse, None, None]:
    """
    Create a Databricks instance pool and clean it up after the test. Returns a function to create instance pools.
    Use `instance_pool_id` attribute from the returned object to get an ID of the pool.

    Keyword Arguments:
    * `instance_pool_name` (str, optional): The name of the instance pool. If not provided, a random name will be generated.
    * `node_type_id` (str, optional): The node type ID of the instance pool. If not provided, a node type with local disk and 16GB memory will be used.
    * other arguments are passed to `WorkspaceClient.instance_pools.create` method.

    Usage:
    ```python
    def test_instance_pool(make_instance_pool):
        logger.info(f"created {make_instance_pool()}")
    ```
    """

    def create(*, instance_pool_name=None, node_type_id=None, **kwargs) -> CreateInstancePoolResponse:
        if instance_pool_name is None:
            instance_pool_name = f"dummy-{make_random(4)}"
        if node_type_id is None:
            node_type_id = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
        pool = ws.instance_pools.create(
            instance_pool_name,
            node_type_id,
            custom_tags={"RemoveAfter": watchdog_remove_after},
            **kwargs,
        )
        log_workspace_link(instance_pool_name, f'compute/instance-pools/{pool.instance_pool_id}', anchor=False)
        return pool

    yield from factory("instance pool", create, lambda pool: ws.instance_pools.delete(pool.instance_pool_id))


@fixture
def make_job(ws, make_random, make_notebook, log_workspace_link, watchdog_remove_after) -> Generator[Job, None, None]:
    """
    Create a Databricks job and clean it up after the test. Returns a function to create jobs.

    Keyword Arguments:
    * `notebook_path` (str, optional): The path to the notebook. If not provided, a random notebook will be created.
    * `name` (str, optional): The name of the job. If not provided, a random name will be generated.
    * `spark_conf` (dict, optional): The Spark configuration of the job.
    * `libraries` (list, optional): The list of libraries to install on the job.
    * other arguments are passed to `WorkspaceClient.jobs.create` method.

    If no task argument is provided, a single task with a notebook task will be created, along with a disposable notebook.
    Latest Spark version and a single worker clusters will be used to run this ephemeral job.

    Usage:
    ```python
    def test_job(make_job):
        logger.info(f"created {make_job()}")
    ```
    """

    def create(notebook_path: str | Path | None = None, **kwargs) -> Job:
        if "name" not in kwargs:
            kwargs["name"] = f"dummy-{make_random(4)}"
        task_spark_conf = kwargs.pop("spark_conf", None)
        libraries = kwargs.pop("libraries", None)
        if isinstance(notebook_path, Path):
            notebook_path = str(notebook_path)
        if not notebook_path:
            notebook_path = make_notebook()
        assert notebook_path is not None
        if "tasks" not in kwargs:
            kwargs["tasks"] = [
                Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                        spark_conf=task_spark_conf,
                    ),
                    notebook_task=NotebookTask(notebook_path=str(notebook_path)),
                    libraries=libraries,
                    timeout_seconds=0,
                )
            ]
        # add RemoveAfter tag for test job cleanup
        date_to_remove = watchdog_remove_after
        remove_after_tag = {"key": "RemoveAfter", "value": date_to_remove}
        if 'tags' not in kwargs:
            kwargs["tags"] = [remove_after_tag]
        else:
            kwargs["tags"].append(remove_after_tag)
        job = ws.jobs.create(**kwargs)
        log_workspace_link(kwargs["name"], f'job/{job.job_id}', anchor=False)
        return job

    yield from factory("job", create, lambda item: ws.jobs.delete(item.job_id))


@fixture
def make_pipeline(
    ws,
    make_random,
    make_notebook,
    watchdog_remove_after,
    watchdog_purge_suffix,
) -> Generator[CreatePipelineResponse, None, None]:
    """
    Create Delta Live Table Pipeline and clean it up after the test. Returns a function to create pipelines.
    Results in a `databricks.sdk.service.pipelines.CreatePipelineResponse` instance.

    Keyword Arguments:
    * `name` (str, optional): The name of the pipeline. If not provided, a random name will be generated.
    * `libraries` (list, optional): The list of libraries to install on the pipeline. If not provided, a random disposable notebook will be created.
    * `clusters` (list, optional): The list of clusters to use for the pipeline. If not provided, a single node cluster will be created with 16GB memory and local disk.

    Usage:
    ```python
    def test_pipeline(make_pipeline, make_pipeline_permissions, make_group):
        group = make_group()
        pipeline = make_pipeline()
        make_pipeline_permissions(
            object_id=pipeline.pipeline_id,
            permission_level=PermissionLevel.CAN_MANAGE,
            group_name=group.display_name,
        )
    ```
    """

    def create(**kwargs) -> CreatePipelineResponse:
        if "name" not in kwargs:
            kwargs["name"] = f"sdk-{make_random(4)}-{watchdog_purge_suffix}"
        if "libraries" not in kwargs:
            notebook_library = NotebookLibrary(path=make_notebook().as_posix())
            kwargs["libraries"] = [PipelineLibrary(notebook=notebook_library)]
        if "clusters" not in kwargs:
            kwargs["clusters"] = [
                PipelineCluster(
                    node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
                    label="default",
                    num_workers=1,
                    custom_tags={"cluster_type": "default", "RemoveAfter": watchdog_remove_after},
                )
            ]
        return ws.pipelines.create(continuous=False, **kwargs)

    yield from factory("delta live table", create, lambda item: ws.pipelines.delete(item.pipeline_id))


@fixture
def make_warehouse(ws, make_random, watchdog_remove_after) -> Generator[Wait[GetWarehouseResponse], None, None]:
    """
    Create a Databricks warehouse and clean it up after the test. Returns a function to create warehouses.

    Keyword Arguments:
    * `warehouse_name` (str, optional): The name of the warehouse. If not provided, a random name will be generated.
    * `warehouse_type` (CreateWarehouseRequestWarehouseType, optional): The type of the warehouse. Defaults to `PRO`.
    * `cluster_size` (str, optional): The size of the cluster. Defaults to `2X-Small`.

    Usage:
    ```python
    def test_warehouse_has_remove_after_tag(ws, make_warehouse):
        new_warehouse = make_warehouse()
        created_warehouse = ws.warehouses.get(new_warehouse.response.id)
        warehouse_tags = created_warehouse.tags.as_dict()
        assert warehouse_tags["custom_tags"][0]["key"] == "RemoveAfter"
    ```
    """

    def create(
        *,
        warehouse_name: str | None = None,
        warehouse_type: CreateWarehouseRequestWarehouseType | None = None,
        cluster_size: str | None = None,
        max_num_clusters: int = 1,
        enable_serverless_compute: bool = False,
        **kwargs,
    ) -> Wait[GetWarehouseResponse]:
        if warehouse_name is None:
            warehouse_name = f"dummy-{make_random(4)}"
        if warehouse_type is None:
            warehouse_type = CreateWarehouseRequestWarehouseType.PRO
        if cluster_size is None:
            cluster_size = "2X-Small"

        remove_after_tags = EndpointTags(custom_tags=[EndpointTagPair(key="RemoveAfter", value=watchdog_remove_after)])
        return ws.warehouses.create(
            name=warehouse_name,
            cluster_size=cluster_size,
            warehouse_type=warehouse_type,
            max_num_clusters=max_num_clusters,
            enable_serverless_compute=enable_serverless_compute,
            tags=remove_after_tags,
            **kwargs,
        )

    yield from factory("warehouse", create, lambda item: ws.warehouses.delete(item.id))
