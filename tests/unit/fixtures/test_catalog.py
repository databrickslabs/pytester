from databricks.sdk.service.catalog import TableInfo, TableType, DataSourceFormat

from databricks.labs.pytester.fixtures.unwrap import call_stateful
from databricks.labs.pytester.fixtures.catalog import make_table


def test_make_table_no_args():
    ctx, table_info = call_stateful(make_table)

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='ucx_trandom',
        table_type=TableType.MANAGED,
        data_source_format=DataSourceFormat.DELTA,
        full_name='hive_metastore.dummy_srandom.ucx_trandom',
        storage_location='dbfs:/user/hive/warehouse/dummy_srandom/ucx_trandom',
        properties={'RemoveAfter': '2024091313'},
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        "CREATE TABLE hive_metastore.dummy_srandom.ucx_trandom (id INT, value STRING) TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.ucx_trandom',
    ]
