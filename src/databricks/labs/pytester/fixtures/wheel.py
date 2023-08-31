import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.workspace import ImportFormat

from pathlib import Path
from typing import Optional


def find_dir_with_leaf(folder: Path, leaf: str) -> Optional[Path]:
    root = folder.root
    while str(folder.absolute()) != root:
        if (folder / leaf).exists():
            return folder
        folder = folder.parent
    return None


def find_project_root(folder: Path) -> Optional[Path]:
    for leaf in ['pyproject.toml', 'setup.py']:
        root = find_dir_with_leaf(folder, leaf)
        if root is not None:
            return root
    return None


def build_wheel_in(project_path: Path, out_path: Path) -> Path:
    try:
        subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir",
             out_path.absolute(), project_path.absolute()],
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        if e.stderr is not None:
            sys.stderr.write(e.stderr.decode())
        raise RuntimeError(e.output.decode().strip()) from None

    found_wheels = list(out_path.glob(f"*.whl"))
    if not found_wheels:
        msg = f"cannot find *.whl in {out_path}"
        raise RuntimeError(msg)
    if len(found_wheels) > 1:
        conflicts = ", ".join(str(whl) for whl in found_wheels)
        msg = f"more than one wheel match: {conflicts}"
        raise RuntimeError(msg)
    wheel_file = found_wheels[0]

    return wheel_file


@pytest.fixture
def fresh_local_wheel_file(tmp_path) -> Path:
    project_root = find_project_root(Path(os.getcwd()))
    build_root = tmp_path / fresh_local_wheel_file.__name__
    shutil.copytree(project_root, build_root)

    return build_wheel_in(build_root, tmp_path / 'dist')


@pytest.fixture
def workspace_library(ws, fresh_local_wheel_file, make_random):
    my_user = ws.current_user.me().user_name
    workspace_folder = f"/Users/{my_user}/wheels/{make_random(10)}"
    ws.workspace.mkdirs(workspace_folder)

    wheel_path = f"{workspace_folder}/{fresh_local_wheel_file.name}"
    with fresh_local_wheel_file.open("rb") as f:
        ws.workspace.upload(wheel_path, f, format=ImportFormat.AUTO)

    yield f'/Workspace/{wheel_path}'

    ws.workspace.delete(workspace_folder, recursive=True)
