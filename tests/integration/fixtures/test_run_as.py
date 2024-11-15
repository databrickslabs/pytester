from pytest import fixture


@fixture
def ws(make_run_as):
    run_as = make_run_as(account_groups=['role.labs.lsql.write'])
    return run_as.ws


def test_creating_notebook_on_behalf_of_ephemeral_principal(make_notebook):
    notebook = make_notebook()
    assert notebook.exists()
