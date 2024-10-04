import io

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.pytester.fixtures.notebooks import make_notebook, make_directory, make_repo
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_notebook_no_args() -> None:
    ctx, notebook = call_stateful(make_notebook)
    assert ctx is not None
    assert notebook is not None
    # First part is the root slash
    assert "/".join(notebook.parts)[1:] == "/Users/test-user/dummy-RANDOM-XXXXX"
    assert not notebook.suffix
    assert notebook.read_text() == "print(1)"
    uri = notebook.as_uri()
    assert uri == 'https://adb-12345679.10.azuredatabricks.net/#workspace/Users/test-user/dummy-RANDOM-XXXXX'


@pytest.mark.parametrize("argument", ["content", "encoding", "language"])
def test_make_notebook_with_path_and_content_or_encoding_or_language_raises_value_error(argument: str) -> None:
    with pytest.raises(
        ValueError, match="The `path` parameter is exclusive with the `content`, `language` and `encoding` parameters."
    ):
        call_stateful(make_notebook, path="test.py", **{argument: "foo"})  # type: ignore


def test_make_notebook_with_path() -> None:
    _, notebook = call_stateful(make_notebook, path="test.py")
    assert notebook.name == "test.py"


def test_make_notebook_with_text_content() -> None:
    _, notebook = call_stateful(make_notebook, content="print(2)")
    assert notebook.read_text() == "print(2)"


def test_make_notebook_with_bytes_content() -> None:
    _, notebook = call_stateful(make_notebook, content=b"print(2)")
    assert notebook.read_bytes() == b"print(2)"


def test_make_notebook_with_io_bytes_content() -> None:
    _, notebook = call_stateful(make_notebook, content=io.BytesIO(b"print(2)"))
    assert notebook.read_bytes() == b"print(2)"


def test_make_notebook_with_sql_language() -> None:
    _, notebook = call_stateful(make_notebook, language=Language.SQL)
    assert notebook.read_text() == "SELECT 1"


def test_make_directory_no_args():
    ctx, directory = call_stateful(make_directory)
    assert ctx is not None
    assert directory is not None


def test_make_repo_no_args():
    ctx, repo = call_stateful(make_repo)
    assert ctx is not None
    assert repo is not None
