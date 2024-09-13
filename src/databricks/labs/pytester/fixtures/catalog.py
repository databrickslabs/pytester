import logging
from collections.abc import Generator, Callable
from pytest import fixture
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import (
    FunctionInfo,
    SchemaInfo,
    TableInfo,
    TableType,
    DataSourceFormat,
    CatalogInfo,
    StorageCredentialInfo,
    AwsIamRoleRequest,
    AzureServicePrincipal,
)
from databricks.sdk.service.compute import Language
from databricks.labs.pytester.fixtures.baseline import factory

logger = logging.getLogger(__name__)


def escape_sql_identifier(path: str, *, maxsplit: int = 2) -> str:
    """
    Escapes the path components to make them SQL safe.

    Args:
        path (str): The dot-separated path of a catalog object.
        maxsplit (int): The maximum number of splits to perform.

    Returns:
         str: The path with all parts escaped in backticks.
    """
    if not path:
        return path
    parts = path.split(".", maxsplit=maxsplit)
    escaped = [f"`{part.strip('`').replace('`', '``')}`" for part in parts]
    return ".".join(escaped)


@fixture
# pylint: disable-next=too-many-statements,too-complex
def make_table(
    sql_backend,
    make_schema,
    make_random,
    log_workspace_link,
    watchdog_remove_after,
) -> Generator[Callable[..., TableInfo], None, None]:
    """
    Create a table and return its info. Remove it after the test. Returns instance of `databricks.sdk.service.catalog.TableInfo`.

    Keyword Arguments:
    * `catalog_name` (str): The name of the catalog where the table will be created. Default is `hive_metastore`.
    * `schema_name` (str): The name of the schema where the table will be created. Default is a random string.
    * `name` (str): The name of the table. Default is a random string.
    * `ctas` (str): The CTAS statement to create the table. Default is `None`.
    * `non_delta` (bool): If `True`, the table will be created as a non-delta table. Default is `False`.
    * `external` (bool): If `True`, the table will be created as an external table. Default is `False`.
    * `external_csv` (str): The location of the external CSV table. Default is `None`.
    * `external_delta` (str): The location of the external Delta table. Default is `None`.
    * `view` (bool): If `True`, the table will be created as a view. Default is `False`.
    * `tbl_properties` (dict): The table properties. Default is `None`.
    * `hiveserde_ddl` (str): The DDL statement to create the table. Default is `None`.
    * `storage_override` (str): The storage location override. Default is `None`.
    * `columns` (list): The list of columns. Default is `None`.

    Usage:
    ```python
    def test_catalog_fixture(make_catalog, make_schema, make_table):
        from_catalog = make_catalog()
        from_schema = make_schema(catalog_name=from_catalog.name)
        from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
        logger.info(f"Created new schema: {from_table_1}")
    ```
    """

    def generate_sql_schema(columns: list[tuple[str, str]]) -> str:
        """Generate a SQL schema from columns."""
        schema = "("
        for index, (col_name, type_name) in enumerate(columns):
            schema += escape_sql_identifier(col_name or str(index), maxsplit=0)
            schema += f" {type_name}, "
        schema = schema[:-2] + ")"  # Remove the last ', '
        return schema

    def generate_sql_column_casting(existing_columns: list[tuple[str, str]], new_columns: list[tuple[str, str]]) -> str:
        """Generate the SQL to cast columns"""
        if len(new_columns) > len(existing_columns):
            raise ValueError(f"Too many columns: {new_columns}")
        select_expressions = []
        for index, ((existing_name, _), (new_name, new_type)) in enumerate(zip(existing_columns, new_columns)):
            column_name_new = escape_sql_identifier(new_name or str(index), maxsplit=0)
            select_expression = f"CAST({existing_name} AS {new_type}) AS {column_name_new}"
            select_expressions.append(select_expression)
        select = ", ".join(select_expressions)
        return select

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
        columns: list[tuple[str, str]] | None = None,
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
        if columns is None:
            schema = "(id INT, value STRING)"
        else:
            schema = generate_sql_schema(columns)
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
            if columns is None:
                select = "*"
            else:
                # These are the columns from the JSON dataset below
                dataset_columns = [
                    ('calories_burnt', 'STRING'),
                    ('device_id', 'STRING'),
                    ('id', 'STRING'),
                    ('miles_walked', 'STRING'),
                    ('num_steps', 'STRING'),
                    ('timestamp', 'STRING'),
                    ('user_id', 'STRING'),
                    ('value', 'STRING'),
                ]
                select = generate_sql_column_casting(dataset_columns, columns)
            # Modified, otherwise it will identify the table as a DB Dataset
            ddl = (
                f"{ddl} USING json location '{storage_location}' as SELECT {select} FROM "
                f"JSON.`dbfs:/databricks-datasets/iot-stream/data-device`"
            )
        elif external_csv is not None:
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.CSV
            storage_location = external_csv
            ddl = f"{ddl} {schema} USING CSV OPTIONS (header=true) LOCATION '{storage_location}'"
        elif external_delta is not None:
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.DELTA
            storage_location = external_delta
            ddl = f"{ddl} {schema} LOCATION '{storage_location}'"
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
            ddl = f"{ddl} {schema}"
        if tbl_properties:
            tbl_properties.update({"RemoveAfter": watchdog_remove_after})
        else:
            tbl_properties = {"RemoveAfter": watchdog_remove_after}

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
        path = f'explore/data/{table_info.catalog_name}/{table_info.schema_name}/{table_info.name}'
        log_workspace_link(f'{table_info.full_name} schema', path)
        return table_info

    def remove(table_info: TableInfo):
        try:
            sql_backend.execute(f"DROP TABLE IF EXISTS {table_info.full_name}")
        except DatabricksError as e:
            if "Cannot drop a view" in str(e):
                sql_backend.execute(f"DROP VIEW IF EXISTS {table_info.full_name}")
            else:
                raise e

    yield from factory("table", create, remove)


@fixture
def make_schema(
    sql_backend,
    make_random,
    log_workspace_link,
    watchdog_remove_after,
) -> Generator[Callable[..., SchemaInfo], None, None]:
    """
    Create a schema and return its info. Remove it after the test. Returns instance of `databricks.sdk.service.catalog.SchemaInfo`.

    Keyword Arguments:
    * `catalog_name` (str): The name of the catalog where the schema will be created. Default is `hive_metastore`.
    * `name` (str): The name of the schema. Default is a random string.

    Usage:
    ```python
    def test_catalog_fixture(make_catalog, make_schema, make_table):
        from_catalog = make_catalog()
        from_schema = make_schema(catalog_name=from_catalog.name)
        from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
        logger.info(f"Created new schema: {from_table_1}")
    ```
    """

    def create(*, catalog_name: str = "hive_metastore", name: str | None = None) -> SchemaInfo:
        if name is None:
            name = f"dummy_S{make_random(4)}".lower()
        full_name = f"{catalog_name}.{name}".lower()
        sql_backend.execute(f"CREATE SCHEMA {full_name} WITH DBPROPERTIES (RemoveAfter={watchdog_remove_after})")
        schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name)
        path = f'explore/data/{schema_info.catalog_name}/{schema_info.name}'
        log_workspace_link(f'{schema_info.full_name} schema', path)
        return schema_info

    def remove(schema_info: SchemaInfo):
        sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_info.full_name} CASCADE")

    yield from factory("schema", create, remove)


@fixture
def make_catalog(ws, sql_backend, make_random, log_workspace_link) -> Generator[Callable[..., CatalogInfo], None, None]:
    """
    Create a catalog and return its info. Remove it after the test.
    Returns instance of `databricks.sdk.service.catalog.CatalogInfo`.

    Usage:
    ```python
    def test_catalog_fixture(make_catalog, make_schema, make_table):
        from_catalog = make_catalog()
        from_schema = make_schema(catalog_name=from_catalog.name)
        from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
        logger.info(f"Created new schema: {from_table_1}")
    ```
    """

    def create() -> CatalogInfo:
        # Warning: As of 2024-09-04 there is no way to mark this catalog for protection against the watchdog.
        # Ref: https://github.com/databrickslabs/watchdog/blob/cdc97afdac1567e89d3b39d938f066fd6267b3ba/scan/objects/uc.go#L68
        name = f"dummy_C{make_random(4)}".lower()
        sql_backend.execute(f"CREATE CATALOG {name}")
        catalog_info = ws.catalogs.get(name)
        log_workspace_link(f'{name} catalog', f'explore/data/{name}')
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
    """
    Create a UDF and return its info. Remove it after the test. Returns instance of `databricks.sdk.service.catalog.FunctionInfo`.

    Keyword Arguments:
    * `catalog_name` (str): The name of the catalog where the UDF will be created. Default is `hive_metastore`.
    * `schema_name` (str): The name of the schema where the UDF will be created. Default is a random string.
    * `name` (str): The name of the UDF. Default is a random string.
    * `hive_udf` (bool): If `True`, the UDF will be created as a Hive UDF. Default is `False`.

    Usage:
    ```python
    def test_make_some_udfs(make_schema, make_udf):
        schema_a = make_schema(catalog_name="hive_metastore")
        make_udf(schema_name=schema_a.name)
        make_udf(schema_name=schema_a.name, hive_udf=True)
    ```
    """

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
        sql_backend.execute(f"DROP FUNCTION IF EXISTS {udf_info.full_name}")

    yield from factory("table", create, remove)


@fixture
def make_storage_credential(ws) -> Generator[Callable[..., StorageCredentialInfo], None, None]:
    """
    Create a storage credential and return its info. Remove it after the test. Returns instance of `databricks.sdk.service.catalog.StorageCredentialInfo`.

    Keyword Arguments:
    * `credential_name` (str): The name of the storage credential. Default is a random string.
    * `application_id` (str): The application ID for the Azure service principal. Default is an empty string.
    * `client_secret` (str): The client secret for the Azure service principal. Default is an empty string.
    * `directory_id` (str): The directory ID for the Azure service principal. Default is an empty string.
    * `aws_iam_role_arn` (str): The ARN of the AWS IAM role. Default is an empty string.
    * `read_only` (bool): If `True`, the storage credential will be read-only. Default is `False`.

    Usage:
    ```python
    def test_storage_credential(env_or_skip, make_storage_credential, make_random):
        random = make_random(6).lower()
        credential_name = f"dummy-{random}"
        make_storage_credential(
            credential_name=credential_name,
            aws_iam_role_arn=env_or_skip("TEST_UBER_ROLE_ID"),
        )
    ```
    """

    def create(
        *,
        credential_name: str,
        application_id: str = "",
        client_secret: str = "",
        directory_id: str = "",
        aws_iam_role_arn: str = "",
        read_only=False,
    ) -> StorageCredentialInfo:
        if aws_iam_role_arn != "":
            storage_credential = ws.storage_credentials.create(
                credential_name, aws_iam_role=AwsIamRoleRequest(role_arn=aws_iam_role_arn), read_only=read_only
            )
        else:
            azure_service_principal = AzureServicePrincipal(directory_id, application_id, client_secret)
            storage_credential = ws.storage_credentials.create(
                credential_name, azure_service_principal=azure_service_principal, read_only=read_only
            )
        return storage_credential

    def remove(storage_credential: StorageCredentialInfo):
        ws.storage_credentials.delete(storage_credential.name, force=True)

    yield from factory("storage_credential", create, remove)
