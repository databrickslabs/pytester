import json
import warnings
from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import Mock

from pytest import fixture
from databricks.sdk.service._internal import Wait
from databricks.sdk.service.compute import (
    CreatePolicyResponse,
    ClusterDetails,
    ClusterSpec,
    CreateInstancePoolResponse,
    Library,
)
from databricks.sdk.service.jobs import Job, JobSettings, NotebookTask, SparkPythonTask, Task
from databricks.sdk.service.pipelines import CreatePipelineResponse, PipelineLibrary, NotebookLibrary, PipelineCluster
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    EndpointTags,
    EndpointTagPair,
    GetWarehouseResponse,
)

from databricks.labs.pytester.fixtures.baseline import factory


@fixture
def make_cluster_policy(
    ws,
    make_random,
    log_workspace_link,
    watchdog_purge_suffix,
) -> Generator[Callable[..., CreatePolicyResponse], None, None]:
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
def make_cluster(
    ws, make_random, log_workspace_link, watchdog_remove_after
) -> Generator[Callable[..., Wait[ClusterDetails]], None, None]:
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
) -> Generator[Callable[..., CreateInstancePoolResponse], None, None]:
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
def make_job(
    ws,
    make_random,
    make_notebook,
    make_workspace_file,
    log_workspace_link,
    watchdog_remove_after,
) -> Generator[Callable[..., Job], None, None]:
    """
    Create a Databricks job and clean it up after the test. Returns a function to create jobs, that returns
    a `databricks.sdk.service.jobs.Job` instance.

    Keyword Arguments:
    * `name` (str, optional): The name of the job. If not provided, a random name will be generated.
    * `path` (str, optional): The path to the notebook or file used in the job. If not provided, a random notebook or file will be created
    * [DEPRECATED: Use `path` instead] `notebook_path` (str, optional): The path to the notebook. If not provided, a random notebook will be created.
    * `content` (str | bytes, optional): The content of the notebook or file used in the job. If not provided, default content of `make_notebook` will be used.
    * `task_type` (type[NotebookTask] | type[SparkPythonTask], optional): The type of task. If not provides, `type[NotebookTask]` will be used.
    * `instance_pool_id` (str, optional): The instance pool id to add to the job cluster. If not provided, no instance pool will be used.
    * `spark_conf` (dict, optional): The Spark configuration of the job. If not provided, Spark configuration is not explicitly set.
    * `libraries` (list, optional): The list of libraries to install on the job.
    * `tags` (list[str], optional): A list of job tags. If not provided, no additional tags will be set on the job.
    * `tasks` (list[Task], optional): A list of job tags. If not provided, a single task with a notebook task will be
       created, along with a disposable notebook. Latest Spark version and a single worker clusters will be used to run
       this ephemeral job.

    Usage:
    ```python
    def test_job(make_job):
        logger.info(f"created {make_job()}")
    ```
    """

    def create(  # pylint: disable=too-many-arguments
        *,
        name: str | None = None,
        path: str | Path | None = None,
        notebook_path: str | Path | None = None,  # DEPRECATED
        content: str | bytes | None = None,
        task_type: type[NotebookTask] | type[SparkPythonTask] = NotebookTask,
        spark_conf: dict[str, str] | None = None,
        instance_pool_id: str | None = None,
        libraries: list[Library] | None = None,
        tags: dict[str, str] | None = None,
        tasks: list[Task] | None = None,
    ) -> Job:
        if notebook_path is not None:
            warnings.warn(
                "The `notebook_path` parameter is replaced with the more general `path` parameter "
                "when introducing workspace paths to python scripts.",
                DeprecationWarning,
            )
            path = notebook_path
        if path and content:
            raise ValueError("The `path` and `content` parameters are exclusive.")
        if tasks and (path or content or spark_conf or libraries):
            raise ValueError(
                "The `tasks` parameter is exclusive with the `path`, `content` `spark_conf` and `libraries` parameters."
            )
        name = name or f"dummy-j{make_random(4)}"
        tags = tags or {}
        tags["RemoveAfter"] = tags.get("RemoveAfter", watchdog_remove_after)
        if not tasks:
            node_type_id = None
            if instance_pool_id is None:
                node_type_id = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
            task = Task(
                task_key=make_random(4),
                description=make_random(4),
                new_cluster=ClusterSpec(
                    num_workers=1,
                    node_type_id=node_type_id,
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    instance_pool_id=instance_pool_id,
                    spark_conf=spark_conf,
                ),
                libraries=libraries,
                timeout_seconds=0,
            )
            if task_type == SparkPythonTask:
                path = path or make_workspace_file(content=content)
                task.spark_python_task = SparkPythonTask(python_file=str(path))
            else:
                path = path or make_notebook(content=content)
                task.notebook_task = NotebookTask(notebook_path=str(path))
            tasks = [task]
        response = ws.jobs.create(name=name, tasks=tasks, tags=tags)
        log_workspace_link(name, f"job/{response.job_id}", anchor=False)
        job = ws.jobs.get(response.job_id)
        if isinstance(response, Mock):  # For testing
            job = Job(settings=JobSettings(name=name, tasks=tasks, tags=tags))
        return job

    yield from factory("job", create, lambda item: ws.jobs.delete(item.job_id))


@fixture
def make_pipeline(
    ws,
    make_random,
    make_notebook,
    watchdog_remove_after,
    watchdog_purge_suffix,
) -> Generator[Callable[..., CreatePipelineResponse], None, None]:
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
def make_warehouse(
    ws, make_random, watchdog_remove_after
) -> Generator[Callable[..., Wait[GetWarehouseResponse]], None, None]:
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
