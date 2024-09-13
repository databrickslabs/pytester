from databricks.labs.pytester.fixtures.secrets import make_secret_scope, make_secret_scope_acl
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_secret_scope_no_args():
    ctx, secret_scope = call_stateful(make_secret_scope)
    assert ctx is not None
    assert secret_scope is not None


def test_make_secret_scope_acl_no_args():
    ctx, secret_scope_acl = call_stateful(make_secret_scope_acl, scope='foo', principal='bar', permission='read')
    assert ctx is not None
    assert secret_scope_acl is not None
