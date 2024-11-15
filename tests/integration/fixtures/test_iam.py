from databricks.sdk.service.workspace import ObjectType


def test_new_user(make_user, ws):
    new_user = make_user()
    home_dir = ws.workspace.get_status(f"/Users/{new_user.user_name}")
    assert home_dir.object_type == ObjectType.DIRECTORY


def test_new_group(make_group, make_user, ws):
    user = make_user()
    group = make_group(members=[user.id])
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
