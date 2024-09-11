import logging
from collections.abc import Generator, Callable
from pytest import fixture
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import FunctionInfo, SchemaInfo, TableInfo, TableType, DataSourceFormat, CatalogInfo
from databricks.sdk.service.compute import Language
from databricks.labs.pytester.fixtures.baseline import factory, get_test_purge_time

logger = logging.getLogger(__name__)


@fixture
# pylint: disable-next=too-many-statements,too-complex
def make_table(ws, sql_backend, make_schema, make_random) -> Generator[Callable[..., TableInfo], None, None]:
    def create(  # pylint: disable=too-many-locals,too-many-arguments,too-many-statements
        *,
        catalog_name="hive_metastore",
        schema_name: str | None = None,
        name: str | None = None,
        ctas: str | None = None,
        non_delta: bool = False,
        external: bool = False,
        external_csv: str | None = None,
        external_delta: str | None = None,
        view: bool = False,
        tbl_properties: dict[str, str] | None = None,
        hiveserde_ddl: str | None = None,
        storage_override: str | None = None,
    ) -> TableInfo:
        if schema_name is None:
            schema = make_schema(catalog_name=catalog_name)
            catalog_name = schema.catalog_name
            schema_name = schema.name
        if name is None:
            name = f"ucx_T{make_random(4)}".lower()
        table_type: TableType | None = None
        data_source_format = None
        storage_location = None
        view_text = None
        full_name = f"{catalog_name}.{schema_name}.{name}".lower()
        ddl = f'CREATE {"VIEW" if view else "TABLE"} {full_name}'
        if view:
            table_type = TableType.VIEW
            view_text = ctas
        if ctas is not None:
            # temporary (if not view)
            ddl = f"{ddl} AS {ctas}"
        elif non_delta:
            table_type = TableType.EXTERNAL  # pylint: disable=redefined-variable-type
            data_source_format = DataSourceFormat.JSON
            # DBFS locations are not purged; no suffix necessary.
            storage_location = f"dbfs:/tmp/ucx_test_{make_random(4)}"
            # Modified, otherwise it will identify the table as a DB Dataset
            ddl = (
                f"{ddl} USING json location '{storage_location}' as SELECT * FROM "
                f"JSON.`dbfs:/databricks-datasets/iot-stream/data-device`"
            )
        elif external_csv is not None:
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.CSV
            storage_location = external_csv
            ddl = f"{ddl} USING CSV OPTIONS (header=true) LOCATION '{storage_location}'"
        elif external_delta is not None:
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.DELTA
            storage_location = external_delta
            ddl = f"{ddl} (id string) LOCATION '{storage_location}'"
        elif external:
            # external table
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.DELTASHARING
            url = "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share"
            storage_location = f"{url}#delta_sharing.default.lending_club"
            ddl = f"{ddl} USING deltaSharing LOCATION '{storage_location}'"
        else:
            # managed table
            table_type = TableType.MANAGED
            data_source_format = DataSourceFormat.DELTA
            storage_location = f"dbfs:/user/hive/warehouse/{schema_name}/{name}"
            ddl = f"{ddl} (id INT, value STRING)"
        if tbl_properties:
            tbl_properties.update({"RemoveAfter": get_test_purge_time()})
        else:
            tbl_properties = {"RemoveAfter": get_test_purge_time()}

        str_properties = ",".join([f" '{k}' = '{v}' " for k, v in tbl_properties.items()])

        # table properties fails with CTAS statements
        alter_table_tbl_properties = ""
        if ctas or non_delta:
            alter_table_tbl_properties = (
                f'ALTER {"VIEW" if view else "TABLE"} {full_name} SET TBLPROPERTIES ({str_properties})'
            )
        else:
            ddl = f"{ddl} TBLPROPERTIES ({str_properties})"

        if hiveserde_ddl:
            ddl = hiveserde_ddl
            data_source_format = None
            table_type = TableType.EXTERNAL
            storage_location = storage_override

        sql_backend.execute(ddl)

        # CTAS AND NON_DELTA does not support TBLPROPERTIES
        if ctas or non_delta:
            sql_backend.execute(alter_table_tbl_properties)

        table_info = TableInfo(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=name,
            full_name=full_name,
            properties=tbl_properties,
            storage_location=storage_location,
            table_type=table_type,
            view_definition=view_text,
            data_source_format=data_source_format,
        )
        logger.info(
            f"Table {table_info.full_name}: "
            f"{ws.config.host}/explore/data/{table_info.catalog_name}/{table_info.schema_name}/{table_info.name}"
        )
        return table_info

    def remove(table_info: TableInfo):
        try:
            sql_backend.execute(f"DROP TABLE IF EXISTS {table_info.full_name}")
        except RuntimeError as e:
            if "Cannot drop a view" in str(e):
                sql_backend.execute(f"DROP VIEW IF EXISTS {table_info.full_name}")
            elif "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("table", create, remove)


@fixture
def make_schema(ws, sql_backend, make_random) -> Generator[Callable[..., SchemaInfo], None, None]:
    def create(*, catalog_name: str = "hive_metastore", name: str | None = None) -> SchemaInfo:
        if name is None:
            name = f"ucx_S{make_random(4)}".lower()
        full_name = f"{catalog_name}.{name}".lower()
        sql_backend.execute(f"CREATE SCHEMA {full_name} WITH DBPROPERTIES (RemoveAfter={get_test_purge_time()})")
        schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name)
        logger.info(
            f"Schema {schema_info.full_name}: "
            f"{ws.config.host}/explore/data/{schema_info.catalog_name}/{schema_info.name}"
        )
        return schema_info

    def remove(schema_info: SchemaInfo):
        try:
            sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_info.full_name} CASCADE")
        except RuntimeError as e:
            if "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("schema", create, remove)


@fixture
def make_catalog(ws, sql_backend, make_random) -> Generator[Callable[..., CatalogInfo], None, None]:
    def create() -> CatalogInfo:
        # Warning: As of 2024-09-04 there is no way to mark this catalog for protection against the watchdog.
        # Ref: https://github.com/databrickslabs/watchdog/blob/cdc97afdac1567e89d3b39d938f066fd6267b3ba/scan/objects/uc.go#L68
        name = f"ucx_C{make_random(4)}".lower()
        sql_backend.execute(f"CREATE CATALOG {name}")
        catalog_info = ws.catalogs.get(name)
        return catalog_info

    yield from factory(
        "catalog",
        create,
        lambda catalog_info: ws.catalogs.delete(catalog_info.full_name, force=True),
    )


@fixture
def make_udf(
    ws,
    env_or_skip,
    sql_backend,
    make_schema,
    make_random,
) -> Generator[Callable[..., FunctionInfo], None, None]:
    def create(
        *,
        catalog_name="hive_metastore",
        schema_name: str | None = None,
        name: str | None = None,
        hive_udf: bool = False,
    ) -> FunctionInfo:
        if schema_name is None:
            schema = make_schema(catalog_name=catalog_name)
            catalog_name = schema.catalog_name
            schema_name = schema.name

        if name is None:
            name = f"ucx_t{make_random(4).lower()}"

        # Note: the Watchdog does not explicitly scan for functions; they are purged along with their parent schema.
        # As such the function can't be marked (and doesn't need to be if the schema as marked) for purge protection.

        full_name = f"{catalog_name}.{schema_name}.{name}".lower()
        if hive_udf:
            cmd_exec = CommandExecutor(
                ws.clusters,
                ws.command_execution,
                lambda: env_or_skip("TEST_DEFAULT_CLUSTER_ID"),
                language=Language.SQL,
            )
            ddl = f"CREATE FUNCTION {full_name} AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs';"
            cmd_exec.run(ddl)
        else:
            ddl = f"CREATE FUNCTION {full_name}(x INT) RETURNS FLOAT CONTAINS SQL DETERMINISTIC RETURN 0;"
            sql_backend.execute(ddl)
        udf_info = FunctionInfo(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=name,
            full_name=full_name,
        )

        logger.info(f"Function {udf_info.full_name} created")
        return udf_info

    def remove(udf_info: FunctionInfo):
        try:
            sql_backend.execute(f"DROP FUNCTION IF EXISTS {udf_info.full_name}")
        except NotFound as e:
            if "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("udf", create, remove)
