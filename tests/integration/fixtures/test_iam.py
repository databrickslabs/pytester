def test_new_user(make_user, ws):
    new_user = make_user()
    loaded = ws.users.get(new_user.id)
    assert new_user.user_name == loaded.user_name


def test_new_group(make_group, make_user, make_run_as, ws):
    user = make_user()
    service_principal = make_run_as()
    group = make_group(members=[user.id, service_principal.id])
    loaded = ws.groups.get(group.id)
    assert group.display_name == loaded.display_name
    assert group.members == loaded.members


def test_new_account_group(make_acc_group, acc):
    group = make_acc_group()
    loaded = acc.groups.get(group.id)
    assert group.display_name == loaded.display_name


def test_run_as_lower_privilege_user(make_run_as, ws):
    run_as = make_run_as(account_groups=['role.labs.lsql.write'])
    through_query = next(run_as.sql_fetch_all("SELECT CURRENT_USER() AS my_name"))
    current_user = ws.current_user.me()
    assert current_user.user_name != through_query.my_name
