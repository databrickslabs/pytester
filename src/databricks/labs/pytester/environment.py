import logging
import os
import pathlib
import sys
import json

_LOG = logging.getLogger(__name__)


def load_debug_env_if_runs_from_ide(key) -> bool:
    """
    Load environment variables from the debug configuration if running from an IDE.

    This function loads environment variables from a debug configuration file if the code is running from an IDE
    (such as a local development environment). The debug configuration file is located at ~/.databricks/debug-env.json.

    Parameters:
    -----------
    key : str
        The key to identify the set of environment variables in the debug configuration.

    Returns:
    --------
    bool:
        True if the environment variables were loaded, False otherwise.

    Raises:
    ------
    KeyError:
        If the specified key is not found in the debug configuration.

    Usage Example:
    --------------
    To load debug environment variables if running from an IDE:

    .. code-block:: python

        if load_debug_env_if_runs_from_ide("my_key"):
            print("Debug environment variables loaded.")
    """
    if not _is_in_debug():
        return False
    conf_file = pathlib.Path.home() / ".databricks/debug-env.json"
    with conf_file.open("r") as f:
        conf = json.load(f)
        if key not in conf:
            msg = f"{key} not found in ~/.databricks/debug-env.json"
            raise KeyError(msg)
        for env_key, value in conf[key].items():
            os.environ[env_key] = value
    return True


def _is_in_debug() -> bool:
    return os.path.basename(sys.argv[0]) in {
        "_jb_pytest_runner.py",
        "testlauncher.py",
    }
