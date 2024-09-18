from databricks.labs.pytester.fixtures.iam import make_user, make_group, make_acc_group
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_user_no_args():
    ctx, user = call_stateful(make_user)
    assert ctx is not None
    assert user is not None
    ctx['ws'].users.create.assert_called_once()
    ctx['ws'].users.delete.assert_called_once()


def test_make_group_no_args():
    ctx, group = call_stateful(make_group)
    assert ctx is not None
    assert group is not None
    ctx['ws'].groups.create.assert_called_once()
    ctx['ws'].groups.delete.assert_called_once()


def test_make_acc_group_no_args():
    ctx, group = call_stateful(make_acc_group)
    assert ctx is not None
    assert group is not None
    ctx['acc'].groups.create.assert_called_once()
    ctx['acc'].groups.delete.assert_called_once()
