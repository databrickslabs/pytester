import logging

from pytest import fixture, skip
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@fixture
def spark(ws: WorkspaceClient):
    """
    Get Databricks Connect Spark session. Requires `databricks-connect` package to be installed.

    Usage:
    ```python
    def test_databricks_connect(spark):
        rows = spark.sql("SELECT 1").collect()
        assert rows[0][0] == 1
    ```
    """
    if not ws.config.cluster_id:
        skip("No cluster_id found in the environment")
    ws.clusters.ensure_cluster_is_running(ws.config.cluster_id)
    try:
        # pylint: disable-next=import-outside-toplevel
        from databricks.connect import (  # type: ignore[import-untyped]
            DatabricksSession,
        )

        return DatabricksSession.builder.sdkConfig(ws.config).getOrCreate()
    except ImportError:
        skip("Please run `pip install databricks-connect`")
        return None
