from pathlib import Path

import pytest

from databricks.labs.pytester.fixtures.wheel import build_wheel_in, find_dir_with_leaf, fresh_local_wheel_file, workspace_library

__folder__ = Path(__file__).parent


def test_find_dir_with_leaf():
    x = find_dir_with_leaf(Path(__file__), '.gitignore')
    assert x is not None


def test_find_dir_with_leaf_missing():
    x = find_dir_with_leaf(Path(__file__), '.nothing')
    assert x is None


@pytest.mark.parametrize("build_system", ['hatchling', 'poetry', 'setuppy', 'setuptools'])
def test_building_wheels(tmp_path, build_system):
    whl = build_wheel_in(__folder__ / f'resources/{build_system}-whl', tmp_path)
    assert whl.exists()


def test_fresh_wheel_file(fresh_local_wheel_file):
    assert fresh_local_wheel_file is not None
