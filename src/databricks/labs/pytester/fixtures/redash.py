from collections.abc import Generator

from pytest import fixture
from databricks.sdk.service.sql import LegacyQuery

from databricks.labs.pytester.fixtures.baseline import factory


@fixture
def make_query(
    ws,
    make_table,
    make_random,
    log_workspace_link,
    watchdog_purge_suffix,
) -> Generator[LegacyQuery, None, None]:
    """
    Create a query and remove it after the test is done. Returns the `databricks.sdk.service.sql.LegacyQuery` object.

    Keyword Arguments:
    - `query`: The query to be stored. Default is `SELECT * FROM <newly created random table>`.

    Usage:
    ```python
    from databricks.sdk.service.sql import PermissionLevel

    def test_permissions_for_redash(
        make_user,
        make_query,
        make_query_permissions,
    ):
        user = make_user()
        query = make_query()
        make_query_permissions(
            object_id=query.id,
            permission_level=PermissionLevel.CAN_EDIT,
            user_name=user.display_name,
        )
    ```
    """

    def create(sql_query: str | None = None) -> LegacyQuery:
        if sql_query is None:
            table = make_table()
            sql_query = f"SELECT * FROM {table.catalog_name}.{table.schema_name}.{table.name}"
        query_name = f"dummy_query_Q{make_random(4)}_{watchdog_purge_suffix}"
        query = ws.queries_legacy.create(
            name=query_name,
            description="TEST QUERY FOR UCX",
            query=sql_query,
            tags=["original_query_tag"],
        )
        log_workspace_link(f"{query_name} query", f'sql/editor/{query.id}')
        return query

    def remove(query: LegacyQuery):
        ws.queries_legacy.delete(query_id=query.id)

    yield from factory("query", create, remove)
