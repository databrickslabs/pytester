import io
import typing

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.pytester.fixtures.baseline import factory


@pytest.fixture
def make_notebook(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks notebooks.

    This fixture provides a function to manage Databricks notebooks using the provided workspace (ws).
    Notebooks can be created with a specified path and content, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks notebooks.

    Usage Example:
    --------------
    To manage Databricks notebooks using the make_notebook fixture:

    .. code-block:: python

        def test_notebook_management(make_notebook):
            notebook_path = make_notebook()
            assert notebook_path.startswith("/Users/") and notebook_path.endswith(".py")
    """
    def create(*, path: str | None = None, content: typing.BinaryIO | None = None, **kwargs):
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}.py"
        if content is None:
            content = io.BytesIO(b"print(1)")
        ws.workspace.upload(path, content, **kwargs)
        return path

    def cleanup(path):
        ws.workspace.delete(path)

    return factory("notebook", create, cleanup)


@pytest.fixture
def make_directory(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks directories.

    This fixture provides a function to manage Databricks directories using the provided workspace (ws).
    Directories can be created with a specified path, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks directories.

    Usage Example:
    --------------
    To manage Databricks directories using the make_directory fixture:

    .. code-block:: python

        def test_directory_management(make_directory):
            directory_path = make_directory()
            assert directory_path.startswith("/Users/") and not directory_path.endswith(".py")
    """
    def create(*, path: str | None = None):
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
        ws.workspace.mkdirs(path)
        return path

    def cleanup(path):
        ws.workspace.delete(path, recursive=True)

    return factory("directory", create, cleanup)


@pytest.fixture
def make_repo(ws: WorkspaceClient, make_random):
    """
    Fixture to manage Databricks repos.

    This fixture provides a function to manage Databricks repos using the provided workspace (ws).
    Repos can be created with a specified URL, provider, and path, and they will be deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to manage Databricks repos.

    Usage Example:
    --------------
    To manage Databricks repos using the make_repo fixture:

    .. code-block:: python

        def test_repo_management(make_repo):
            repo_info = make_repo()
            assert repo_info is not None
    """
    def create(*, url=None, provider=None, path=None, **kwargs):
        if path is None:
            path = f"/Repos/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
        if url is None:
            url = "https://github.com/shreyas-goenka/empty-repo.git"
        if provider is None:
            provider = "github"
        return ws.repos.create(url, provider, path=path, **kwargs)

    def cleanup(repo_info):
        ws.repos.delete(repo_info.id)

    return factory("repo", create, cleanup)
