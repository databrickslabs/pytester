from databricks.sdk.service.workspace import AclPermission


def test_secret_scope_creation(make_secret_scope):
    secret_scope_name = make_secret_scope()
    assert secret_scope_name.startswith("dummy-")


def test_secret_scope_acl_management(make_user, make_secret_scope, make_secret_scope_acl):
    scope_name = make_secret_scope()
    principal_name = make_user().display_name
    permission = AclPermission.READ

    acl_info = make_secret_scope_acl(
        scope=scope_name,
        principal=principal_name,
        permission=permission,
    )
    assert acl_info == (scope_name, principal_name)


def test_secret_scope_acl(make_secret_scope, make_secret_scope_acl, make_group):
    scope_name = make_secret_scope()
    make_secret_scope_acl(scope=scope_name, principal=make_group().display_name, permission=AclPermission.WRITE)
