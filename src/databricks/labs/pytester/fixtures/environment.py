import json
import os
from pathlib import Path
import sys
from collections.abc import MutableMapping, Callable

import pytest
from pytest import fixture
from databricks.labs.blueprint.entrypoint import find_dir_with_leaf


@fixture
def is_in_debug() -> bool:
    return os.path.basename(sys.argv[0]) in {"_jb_pytest_runner.py", "testlauncher.py"}


@fixture
def debug_env_name():
    """
    Specify the name of the debug environment. By default, it is set to `.env`,
    which will try to find a [file named `.env`](https://www.dotenv.org/docs/security/env)
    in any of the parent directories of the current working directory and load
    the environment variables from it via the [`debug_env` fixture](#debug_env-fixture).

    Alternatively, if you are concerned of the
    [risk of `.env` files getting checked into version control](https://thehackernews.com/2024/08/attackers-exploit-public-env-files-to.html),
    we recommend using the `~/.databricks/debug-env.json` file to store different sets of environment variables.
    The file cannot be checked into version control by design, because it is stored in the user's home directory.

    This file is used for local debugging and integration tests in IDEs like PyCharm, VSCode, and IntelliJ IDEA
    while developing Databricks Platform Automation Stack, which includes Databricks SDKs for Python, Go, and Java,
    as well as Databricks Terraform Provider and Databricks CLI. This file enables multi-environment and multi-cloud
    testing with a single set of integration tests.

    The file is typically structured as follows:

    ```shell
    $ cat ~/.databricks/debug-env.json
    {
       "ws": {
         "CLOUD_ENV": "azure",
         "DATABRICKS_HOST": "....azuredatabricks.net",
         "DATABRICKS_CLUSTER_ID": "0708-200540-...",
         "DATABRICKS_WAREHOUSE_ID": "33aef...",
            ...
       },
       "acc": {
         "CLOUD_ENV": "aws",
         "DATABRICKS_HOST": "accounts.cloud.databricks.net",
         "DATABRICKS_CLIENT_ID": "....",
         "DATABRICKS_CLIENT_SECRET": "....",
         ...
       }
    }
    ```

    And you can load it in your `conftest.py` file as follows:

    ```python
    @pytest.fixture
    def debug_env_name():
        return "ws"
    ```

    This will load the `ws` environment from the `~/.databricks/debug-env.json` file.

    If any of the environment variables are not found, [`env_or_skip` fixture](#env_or_skip-fixture)
    will gracefully skip the execution of tests.
    """
    return ".env"


@fixture
def debug_env(monkeypatch, debug_env_name, is_in_debug) -> MutableMapping[str, str]:
    """
    Loads environment variables specified in [`debug_env_name` fixture](#debug_env_name-fixture) from a file
    for local debugging in IDEs, otherwise allowing the tests to run with the default environment variables
    specified in the CI/CD pipeline.
    """
    if not is_in_debug:
        return os.environ
    if debug_env_name == ".env":
        dot_env = _parse_dotenv()
        if not dot_env:
            return os.environ
        for env_key, value in dot_env.items():
            monkeypatch.setenv(env_key, value)
        return os.environ
    conf_file = Path.home() / ".databricks/debug-env.json"
    if not conf_file.exists():
        return os.environ
    with conf_file.open("r") as f:
        conf = json.load(f)
        if debug_env_name not in conf:
            sys.stderr.write(f"""{debug_env_name} not found in ~/.databricks/debug-env.json""")
            msg = f"{debug_env_name} not found in ~/.databricks/debug-env.json"
            raise KeyError(msg)
        for env_key, value in conf[debug_env_name].items():
            monkeypatch.setenv(env_key, value)
    return os.environ


@fixture
def env_or_skip(debug_env, is_in_debug) -> Callable[[str], str]:
    """
    Fixture to get environment variables or skip tests.

    It is extremely useful to skip tests if the required environment variables are not set.

    In the following example, `test_something` would only run if the environment variable
    `SOME_EXTERNAL_SERVICE_TOKEN` is set:

    ```python
    def test_something(env_or_skip):
        token = env_or_skip("SOME_EXTERNAL_SERVICE_TOKEN")
        assert token is not None
    ```
    """
    skip = pytest.skip
    if is_in_debug:
        skip = pytest.fail  # type: ignore[assignment]

    def inner(var: str) -> str:
        if var not in debug_env:
            skip(f"Environment variable {var} is missing")
        return debug_env[var]

    return inner


def _parse_dotenv():
    """See https://www.dotenv.org/docs/security/env"""
    dot_env = find_dir_with_leaf(Path.cwd(), '.env')
    if dot_env is None:
        return {}
    env_vars = {}
    with (dot_env / '.env').open(encoding='utf8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, value = line.split('=', 1)
            # Remove any surrounding quotes (single or double)
            value = value.strip().strip('"').strip("'")
            env_vars[key.strip()] = value
    return env_vars
