def test_run_as(make_run_as, ws):
    run_as = make_run_as(account_groups=['role.labs.lsql.write'])
    through_query = next(run_as.sql_fetch_all("SELECT CURRENT_USER() AS my_name"))
    me = ws.current_user.me()
    assert me.user_name != through_query.my_name


def test_notebooks_are_created_by_different_users(make_run_as, make_notebook):
    notebook_by_current_user = make_notebook()
    a = notebook_by_current_user.parent.as_posix()

    run_as = make_run_as(account_groups=['role.labs.lsql.write'])
    notebook_by_ephemeral_principal = run_as.make_notebook()
    b = notebook_by_ephemeral_principal.parent.as_posix()

    assert a != b
