import logging

from pytest import fixture, skip
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@fixture
def spark(ws: WorkspaceClient):
    """
    Get Databricks Connect Spark session. Requires `databricks-connect` package to be installed.

    To enable serverless set the local environment variable `DATABRICKS_SERVERLESS_COMPUTE_ID` to `"auto"`.
    If this environment variable is set, Databricks Connect ignores the cluster_id.
    If `DATABRICKS_SERVERLESS_COMPUTE_ID` is set to a specific serverless cluster ID, that cluster will be used instead.
    However, this is not recommended, as serverless clusters are ephemeral by design.
    See more details [here](https://docs.databricks.com/en/dev-tools/databricks-connect/cluster-config.html#configure-a-connection-to-serverless-compute).

    Usage:
    ```python
    def test_databricks_connect(spark):
        rows = spark.sql("SELECT 1").collect()
        assert rows[0][0] == 1
    ```
    """
    cluster_id = ws.config.cluster_id
    serverless_cluster_id = ws.config.serverless_compute_id

    if not serverless_cluster_id:
        ensure_cluster_is_running(cluster_id, ws)

    if serverless_cluster_id and serverless_cluster_id != "auto":
        ensure_cluster_is_running(serverless_cluster_id, ws)

    try:
        # pylint: disable-next=import-outside-toplevel
        from databricks.connect import (  # type: ignore[import-untyped]
            DatabricksSession,
        )

        if serverless_cluster_id:
            logging.debug(f"Using serverless cluster id '{serverless_cluster_id}'")
            return DatabricksSession.builder.serverless(True).getOrCreate()

        logging.debug(f"Using cluster id '{cluster_id}'")
        return DatabricksSession.builder.sdkConfig(ws.config).getOrCreate()
    except ImportError:
        skip("Please run `pip install databricks-connect`")
        return None


def ensure_cluster_is_running(cluster_id: str, ws: WorkspaceClient) -> None:
    if not cluster_id:
        skip("No cluster_id found in the environment")
    ws.clusters.ensure_cluster_is_running(cluster_id)
