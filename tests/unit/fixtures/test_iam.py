from databricks.labs.pytester.fixtures.iam import make_user, make_group
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_user_no_args():
    ctx, user = call_stateful(make_user)
    assert ctx is not None
    assert user is not None


def test_make_group_no_args():
    ctx, group = call_stateful(make_group)
    assert ctx is not None
    assert group is not None
