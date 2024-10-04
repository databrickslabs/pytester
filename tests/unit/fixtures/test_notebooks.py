from databricks.labs.pytester.fixtures.notebooks import make_notebook, make_directory, make_repo
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_notebook_no_args():
    ctx, notebook = call_stateful(make_notebook)
    assert ctx is not None
    assert notebook is not None


def test_make_notebook_with_path() -> None:
    _, notebook = call_stateful(make_notebook, path="test.py")
    assert notebook.name == "test.py"


def test_make_directory_no_args():
    ctx, directory = call_stateful(make_directory)
    assert ctx is not None
    assert directory is not None


def test_make_repo_no_args():
    ctx, repo = call_stateful(make_repo)
    assert ctx is not None
    assert repo is not None
