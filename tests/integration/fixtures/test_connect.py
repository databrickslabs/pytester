import os
from pytest import fixture
from pyspark.sql.session import SparkSession
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


@fixture
def serverless_env():
    os.environ['DATABRICKS_SERVERLESS_COMPUTE_ID'] = "auto"
    yield
    os.environ.pop('DATABRICKS_SERVERLESS_COMPUTE_ID')


@fixture
def debug_env_bugfix(monkeypatch, debug_env):
    # This is a workaround to set shared cluster
    # TODO: Update secret vault for acceptance testing and remove the bugfix
    monkeypatch.setitem(debug_env, "DATABRICKS_CLUSTER_ID", "1114-152544-29g1w07e")


@fixture
def spark_serverless_cluster_id(ws):
    # get new spark session with serverless cluster outside the actual spark fixture under test
    spark_serverless = DatabricksSession.builder.serverless(True).getOrCreate()
    # get cluster id from the existing serverless spark session
    cluster_id = spark_serverless.conf.get("spark.databricks.clusterUsageTags.clusterId")
    ws.config.serverless_compute_id = cluster_id
    yield cluster_id
    spark_serverless.stop()


def test_databricks_connect(debug_env_bugfix, ws, spark):
    rows = spark.sql("SELECT 1").collect()
    assert rows[0][0] == 1
    assert not is_serverless_cluster(spark, ws)


def test_databricks_connect_serverless(serverless_env, ws, spark):
    rows = spark.sql("SELECT 1").collect()
    assert rows[0][0] == 1
    assert is_serverless_cluster(spark, ws)


def test_databricks_connect_serverless_set_cluster_id(ws, spark_serverless_cluster_id, spark):
    rows = spark.sql("SELECT 1").collect()
    assert rows[0][0] == 1

    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    assert spark_serverless_cluster_id == cluster_id
    assert is_serverless_cluster(spark, ws)


def is_serverless_cluster(spark: SparkSession, ws: WorkspaceClient) -> bool:
    """
    Check if the current cluster used is serverless.
    """
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    creator = ws.clusters.get(cluster_id).creator_user_name
    return not creator  # serverless clusters don't have assigned creator
