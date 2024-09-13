import io
import logging
import typing
from collections.abc import Generator
from pathlib import Path

from pytest import fixture
from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk.service.workspace import Language, ImportFormat, RepoInfo
from databricks.sdk import WorkspaceClient

from databricks.labs.pytester.fixtures.baseline import factory

logger = logging.getLogger(__name__)


@fixture
def make_notebook(ws, make_random, watchdog_purge_suffix) -> Generator[WorkspacePath, None, None]:
    """
    Returns a function to create Databricks Notebooks and clean them up after the test.
    The function returns [`os.PathLike` object](https://github.com/databrickslabs/blueprint?tab=readme-ov-file#python-native-pathlibpath-like-interfaces).

    Keyword arguments:
    * `path` (str, optional): The path of the notebook. Defaults to `dummy-*` notebook in current user's home folder.
    * `content` (typing.BinaryIO, optional): The content of the notebook. Defaults to `print(1)`.
    * `language` (`databricks.sdk.service.workspace.Language`, optional): The language of the notebook. Defaults to `Language.PYTHON`.
    * `format` (`databricks.sdk.service.workspace.ImportFormat`, optional): The format of the notebook. Defaults to `ImportFormat.SOURCE`.
    * `overwrite` (bool, optional): Whether to overwrite the notebook if it already exists. Defaults to `False`.

    This example creates a notebook and verifies that `print(1)` is in the content:
    ```python
    def test_creates_some_notebook(make_notebook):
        notebook = make_notebook()
        assert "print(1)" in notebook.read_text()
    ```
    """

    def create(
        *,
        path: str | Path | None = None,
        content: typing.BinaryIO | None = None,
        language: Language = Language.PYTHON,
        format: ImportFormat = ImportFormat.SOURCE,  # pylint:  disable=redefined-builtin
        overwrite: bool = False,
    ) -> WorkspacePath:
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/dummy-{make_random(4)}-{watchdog_purge_suffix}"
        elif isinstance(path, Path):
            path = str(path)
        if content is None:
            content = io.BytesIO(b"print(1)")
        path = str(path)
        ws.workspace.upload(path, content, language=language, format=format, overwrite=overwrite)
        workspace_path = WorkspacePath(ws, path)
        logger.info(f"Created notebook: {workspace_path.as_uri()}")
        return workspace_path

    yield from factory("notebook", create, lambda path: path.unlink(missing_ok=True))


@fixture
def make_directory(ws: WorkspaceClient, make_random, watchdog_purge_suffix) -> Generator[WorkspacePath, None, None]:
    """
    Returns a function to create Databricks Workspace Folders and clean them up after the test.
    The function returns [`os.PathLike` object](https://github.com/databrickslabs/blueprint?tab=readme-ov-file#python-native-pathlibpath-like-interfaces).

    Keyword arguments:
    * `path` (str, optional): The path of the notebook. Defaults to `dummy-*` folder in current user's home folder.

    This example creates a folder and verifies that it contains a notebook:
    ```python
    def test_creates_some_folder_with_a_notebook(make_directory, make_notebook):
        folder = make_directory()
        notebook = make_notebook(path=folder / 'foo.py')
        files = [_.name for _ in folder.iterdir()]
        assert ['foo.py'] == files
        assert notebook.parent == folder
    ```
    """

    def create(*, path: str | Path | None = None) -> WorkspacePath:
        if path is None:
            path = f"~/dummy-{make_random(4)}-{watchdog_purge_suffix}"
        workspace_path = WorkspacePath(ws, path).expanduser()
        workspace_path.mkdir(exist_ok=True)
        logger.info(f"Created folder: {workspace_path.as_uri()}")
        return workspace_path

    yield from factory("directory", create, lambda ws_path: ws_path.rmdir(recursive=True))


@fixture
def make_repo(ws, make_random, watchdog_purge_suffix) -> Generator[RepoInfo, None, None]:
    """
    Returns a function to create Databricks Repos and clean them up after the test.
    The function returns a `databricks.sdk.service.workspace.RepoInfo` object.

    Keyword arguments:
    * `url` (str, optional): The URL of the repository.
    * `provider` (str, optional): The provider of the repository.
    * `path` (str, optional): The path of the repository. Defaults to `/Repos/{current_user}/sdk-{random}-{purge_suffix}`.

    Usage:
    ```python
    def test_repo(make_repo):
        logger.info(f"created {make_repo()}")
    ```
    """

    def create(*, url=None, provider=None, path=None, **kwargs) -> RepoInfo:
        if path is None:
            path = f"/Repos/{ws.current_user.me().user_name}/sdk-{make_random(4)}-{watchdog_purge_suffix}"
        if url is None:
            url = "https://github.com/shreyas-goenka/empty-repo.git"
        if provider is None:
            provider = "github"
        return ws.repos.create(url, provider, path=path, **kwargs)

    yield from factory("repo", create, lambda x: ws.repos.delete(x.id))
