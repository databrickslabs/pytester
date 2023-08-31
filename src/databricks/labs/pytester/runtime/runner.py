# content of myinvoke.py
import sys

import pytest


class DatabricksPytesterPlugin:
    def pytest_sessionfinish(self):
        print("*** test run reporting finishing")


def main():
    # Calling pytest.main() will result in importing your tests and any modules that they import.
    # Due to the caching mechanism of python’s import system, making subsequent calls to pytest.main()
    # from the same process will not reflect changes to those files between the calls. For this reason,
    # making multiple calls to pytest.main() from the same process (in order to re-run tests, for example)
    # is not recommended.
    global dbutils
    dbutils.library.restartPython()
    result = pytest.main(["-qq"], plugins=[DatabricksPytesterPlugin()])
    return result