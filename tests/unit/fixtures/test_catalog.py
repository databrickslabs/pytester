from unittest.mock import ANY

from databricks.sdk.service.catalog import TableInfo, TableType, DataSourceFormat, FunctionInfo, SchemaInfo

from databricks.labs.pytester.fixtures.unwrap import call_stateful
from databricks.labs.pytester.fixtures.catalog import (
    make_table,
    make_udf,
    make_catalog,
    make_storage_credential,
    make_schema,
    make_volume,
)


def test_make_table_no_args():
    ctx, table_info = call_stateful(make_table)

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='dummy_trandom',
        table_type=TableType.MANAGED,
        data_source_format=DataSourceFormat.DELTA,
        full_name='hive_metastore.dummy_srandom.dummy_trandom',
        storage_location='dbfs:/user/hive/warehouse/dummy_srandom/dummy_trandom',
        properties={'RemoveAfter': '2024091313'},
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        "CREATE TABLE hive_metastore.dummy_srandom.dummy_trandom (id INT, value STRING) TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.dummy_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_view():
    ctx, table_info = call_stateful(make_table, view=True, ctas='SELECT 1')

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='dummy_trandom',
        table_type=TableType.VIEW,
        full_name='hive_metastore.dummy_srandom.dummy_trandom',
        properties={'RemoveAfter': '2024091313'},
        view_definition='SELECT 1',
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        "CREATE VIEW hive_metastore.dummy_srandom.dummy_trandom AS SELECT 1",
        "ALTER VIEW hive_metastore.dummy_srandom.dummy_trandom SET TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.dummy_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_external_table():
    ctx, table_info = call_stateful(make_table, non_delta=True, columns=[('id', 'INT'), ('value', 'STRING')])

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='dummy_trandom',
        table_type=TableType.EXTERNAL,
        data_source_format=DataSourceFormat.JSON,
        full_name='hive_metastore.dummy_srandom.dummy_trandom',
        storage_location='dbfs:/tmp/dummy_trandom',
        properties={'RemoveAfter': '2024091313'},
    )

    ctx['log_workspace_link'].assert_called_with(
        'hive_metastore.dummy_srandom.dummy_trandom schema',
        'explore/data/hive_metastore/dummy_srandom/dummy_trandom',
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        'CREATE TABLE hive_metastore.dummy_srandom.dummy_trandom USING json location '
        "'dbfs:/tmp/dummy_trandom' as SELECT CAST(calories_burnt AS INT) AS `id`, "
        'CAST(device_id AS STRING) AS `value` FROM '
        'JSON.`dbfs:/databricks-datasets/iot-stream/data-device`',
        "ALTER TABLE hive_metastore.dummy_srandom.dummy_trandom SET TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.dummy_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_table_custom_schema():
    ctx, table_info = call_stateful(make_table, columns=[('a', 'INT'), ('b', 'STRING')])

    assert table_info == TableInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='dummy_trandom',
        table_type=TableType.MANAGED,
        data_source_format=DataSourceFormat.DELTA,
        full_name='hive_metastore.dummy_srandom.dummy_trandom',
        storage_location='dbfs:/user/hive/warehouse/dummy_srandom/dummy_trandom',
        properties={'RemoveAfter': '2024091313'},
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        "CREATE TABLE hive_metastore.dummy_srandom.dummy_trandom (`a` INT, `b` STRING) TBLPROPERTIES ( 'RemoveAfter' = '2024091313' )",
        'DROP TABLE IF EXISTS hive_metastore.dummy_srandom.dummy_trandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_make_catalog() -> None:
    ctx, info = call_stateful(make_catalog)
    ctx['ws'].catalogs.create.assert_called_once()  # can't specify call params accurately
    assert info.properties and info.properties.get("RemoveAfter", None)


def test_make_catalog_creates_catalog_with_name() -> None:
    ctx, _ = call_stateful(make_catalog, name="test")
    ctx['ws'].catalogs.create.assert_called_once_with(name="test", properties=ANY)


def test_make_udf():
    ctx, fn_info = call_stateful(make_udf)

    assert fn_info == FunctionInfo(
        catalog_name='hive_metastore',
        schema_name='dummy_srandom',
        name='dummy_frandom',
        full_name='hive_metastore.dummy_srandom.dummy_frandom',
    )

    assert ctx['sql_backend'].queries == [
        'CREATE SCHEMA hive_metastore.dummy_srandom WITH DBPROPERTIES (RemoveAfter=2024091313)',
        'CREATE FUNCTION hive_metastore.dummy_srandom.dummy_frandom(x INT) RETURNS '
        'FLOAT CONTAINS SQL DETERMINISTIC RETURN 0;',
        'DROP FUNCTION IF EXISTS hive_metastore.dummy_srandom.dummy_frandom',
        'DROP SCHEMA IF EXISTS hive_metastore.dummy_srandom CASCADE',
    ]


def test_storage_credential():
    ctx, fn_info = call_stateful(make_storage_credential, credential_name='abc')
    assert ctx is not None
    assert fn_info is not None


def test_make_schema() -> None:
    ctx, info = call_stateful(make_schema, name='abc', location='abfss://container1@storage.com')
    assert ctx['sql_backend'].queries == [
        "CREATE SCHEMA hive_metastore.abc LOCATION 'abfss://container1@storage.com' WITH DBPROPERTIES (RemoveAfter=2024091313)",
        "DROP SCHEMA IF EXISTS hive_metastore.abc CASCADE",
    ]
    assert info == SchemaInfo(
        catalog_name='hive_metastore',
        name='abc',
        full_name='hive_metastore.abc',
        storage_location='abfss://container1@storage.com',
    )


def test_make_volume():
    ctx, info = call_stateful(make_volume)

    ctx['ws'].volumes.create.assert_called_once()
    print(info)
    assert info.catalog_name is not None
    assert info.schema_name is not None
    assert info.name is not None


def test_make_volume_with_name():
    ctx, info = call_stateful(make_volume, name='test_volume')

    ctx['ws'].volumes.create.assert_called_once()
    assert info.catalog_name is not None
    assert info.schema_name is not None
    assert info.name == 'test_volume'

