from databricks.labs.pytester.fixtures.redash import make_query
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_query_no_args():
    ctx, query = call_stateful(make_query)
    assert ctx is not None
    assert query is not None
