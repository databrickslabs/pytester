import logging

logger = logging.getLogger(__name__)


def test_creates_some_notebook(make_notebook):
    notebook = make_notebook()
    assert "print(1)" in notebook.read_text()


def test_creates_some_folder_with_a_notebook(make_directory, make_notebook):
    folder = make_directory()
    notebook = make_notebook(path=folder / 'foo.py')
    files = [_.name for _ in folder.iterdir()]
    assert ['foo.py'] == files
    assert notebook.parent == folder


def test_repo(make_repo):
    logger.info(f"created {make_repo()}")
