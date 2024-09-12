from collections.abc import Generator

from pytest import fixture
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.sql import LegacyQuery

from databricks.labs.pytester.fixtures.baseline import get_purge_suffix, factory


@fixture
def make_query(ws, make_table, make_random, log_workspace_link) -> Generator[LegacyQuery, None, None]:
    """
    Create a query and remove it after the test is done. Returns the `databricks.sdk.service.sql.LegacyQuery` object.

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

    def create() -> LegacyQuery:
        table = make_table()
        query_name = f"dummy_query_Q{make_random(4)}_{get_purge_suffix()}"
        query = ws.queries_legacy.create(
            name=query_name,
            description="TEST QUERY FOR UCX",
            query=f"SELECT * FROM {table.schema_name}.{table.name}",
            tags=["original_query_tag"],
        )
        log_workspace_link(f"{query_name} query", f'sql/editor/{query.id}')
        return query

    def remove(query: LegacyQuery):
        try:
            ws.queries_legacy.delete(query_id=query.id)
        except DatabricksError:
            pass

    yield from factory("query", create, remove)
