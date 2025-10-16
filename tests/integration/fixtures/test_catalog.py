import logging
import pytest


logger = logging.getLogger(__name__)


def test_catalog_fixture(make_catalog, make_schema, make_table):
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    logger.info(f"Created new schema: {from_table_1}")


def test_schema_fixture(make_schema):
    logger.info(f"Created new schema: {make_schema()}")
    logger.info(f"Created new schema: {make_schema()}")


@pytest.mark.skip("Invalid configuration value detected for fs.azure.account.key")
def test_managed_schema_fixture(make_schema, make_random, env_or_skip):
    schema_name = f"dummy_s{make_random(4)}".lower()
    schema_location = f"{env_or_skip('TEST_MOUNT_CONTAINER')}/a/{schema_name}"
    logger.info(f"Created new schema: {make_schema(location=schema_location)}")


def test_new_managed_table_in_new_schema(make_table):
    logger.info(f"Created new managed table in new schema: {make_table()}")


def test_new_managed_table_in_default_schema(make_table):
    logger.info(f'Created new managed table in default schema: {make_table(schema_name="default")}')


def test_external_delta_table_in_new_schema(make_table):
    logger.info(f"Created new external table in new schema: {make_table(external=True)}")


def test_external_json_table_in_new_schema(make_table):
    logger.info(f"Created new external JSON table in new schema: {make_table(non_delta=True)}")


def test_table_fixture(make_table):
    logger.info(f'Created new tmp table in new schema: {make_table(ctas="SELECT 2+2 AS four")}')
    logger.info(f'Created table with properties: {make_table(tbl_properties={"test": "tableproperty"})}')


@pytest.mark.skip(reason="fix drop view")
def test_create_view(make_table):
    logger.info(f'Created new view in new schema: {make_table(view=True, ctas="SELECT 2+2 AS four")}')


def test_make_some_udfs(make_schema, make_udf):
    schema_a = make_schema(catalog_name="hive_metastore")
    make_udf(schema_name=schema_a.name)
    make_udf(schema_name=schema_a.name, hive_udf=True)


def test_storage_credential(env_or_skip, make_storage_credential, make_random):
    random = make_random(6).lower()
    credential_name = f"dummy-{random}"
    make_storage_credential(
        credential_name=credential_name,
        aws_iam_role_arn=env_or_skip("TEST_UBER_ROLE_ID"),
    )


def test_make_volume(make_volume):
    logger.info(f"Created new volume: {make_volume()}")


def test_remove_after_property_table(ws, make_table, sql_backend):
    new_table = make_table()
    # TODO: tables.get is currently failing with
    #   databricks.sdk.errors.platform.NotFound: Catalog 'hive_metastore' does not exist.
    sql_response = list(sql_backend.fetch(f"DESCRIBE TABLE EXTENDED {new_table.full_name}"))
    for row in sql_response:
        if row.col_name == "Table Properties":
            assert "RemoveAfter" in row[1]


def test_remove_after_property_schema(ws, make_schema, sql_backend):
    new_schema = make_schema()
    # TODO: schemas.get is currently failing with
    #   databricks.sdk.errors.platform.NotFound: Catalog 'hive_metastore' does not exist.
    sql_response = list(sql_backend.fetch(f"DESCRIBE SCHEMA EXTENDED {new_schema.full_name}"))
    for row in sql_response:
        if row.database_description_item == "Properties":
            assert "RemoveAfter" in row[1]
