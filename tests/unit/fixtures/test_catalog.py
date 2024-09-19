from databricks.sdk.service.catalog import TableInfo, TableType, DataSourceFormat, FunctionInfo

from databricks.labs.pytester.fixtures.unwrap import call_stateful
from databricks.labs.pytester.fixtures.catalog import make_table, make_udf, make_catalog, make_storage_credential


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
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.ucx_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_view():
    ctx, table_info = call_stateful(make_table, view=True, ctas='SELECT 1')

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='ucx_trandom',
        table_type=TableType.VIEW,
        full_name='hive_metastore.dummy_srandom.ucx_trandom',
        properties={'RemoveAfter': '2024091313'},
        view_definition='SELECT 1',
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        "CREATE VIEW hive_metastore.dummy_srandom.ucx_trandom AS SELECT 1",
        "ALTER VIEW hive_metastore.dummy_srandom.ucx_trandom SET TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.ucx_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_external_table():
    ctx, table_info = call_stateful(make_table, non_delta=True, columns=[('id', 'INT'), ('value', 'STRING')])

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='ucx_trandom',
        table_type=TableType.EXTERNAL,
        data_source_format=DataSourceFormat.JSON,
        full_name='hive_metastore.dummy_srandom.ucx_trandom',
        storage_location='dbfs:/tmp/ucx_test_RANDOM',
        properties={'RemoveAfter': '2024091313'},
    )

    ctx['log_workspace_link'].assert_called_with(
        'hive_metastore.dummy_srandom.ucx_trandom schema',
        'explore/data/hive_metastore/dummy_srandom/ucx_trandom',
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        'CREATE TABLE hive_metastore.dummy_srandom.ucx_trandom USING json location '
        "'dbfs:/tmp/ucx_test_RANDOM' as SELECT CAST(calories_burnt AS INT) AS `id`, "
        'CAST(device_id AS STRING) AS `value` FROM '
        'JSON.`dbfs:/databricks-datasets/iot-stream/data-device`',
        "ALTER TABLE hive_metastore.dummy_srandom.ucx_trandom SET TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.ucx_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_table_custom_schema():
    ctx, table_info = call_stateful(make_table, columns=[('a', 'INT'), ('b', 'STRING')])

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
        "CREATE TABLE hive_metastore.dummy_srandom.ucx_trandom (`a` INT, `b` STRING) TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.ucx_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_catalog():
    ctx, info = call_stateful(make_catalog)
    ctx['ws'].catalogs.create.assert_called()  # can't specify call params accurately
    assert info.properties and info.properties.get("RemoveAfter", None)


def test_make_udf():
    ctx, fn_info = call_stateful(make_udf)

    assert fn_info == FunctionInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='ucx_trandom',
        full_name='hive_metastore.dummy_srandom.ucx_trandom',
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        'CREATE FUNCTION hive_metastore.dummy_srandom.ucx_trandom(x INT) RETURNS '
        'FLOAT CONTAINS SQL DETERMINISTIC RETURN 0;',
        'DROP FUNCTION IF EXISTS hive_metastore.dummy_srandom.ucx_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_storage_credential():
    ctx, fn_info = call_stateful(make_storage_credential, credential_name='abc')
    assert ctx is not None
    assert fn_info is not None
