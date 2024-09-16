def test_databricks_connect(spark):
    rows = spark.sql("SELECT 1").collect()
    assert rows[0][0] == 1
