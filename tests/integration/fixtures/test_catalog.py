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
