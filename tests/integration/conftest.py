import logging

from pytest import fixture
from databricks.labs.blueprint.logger import install_logger

install_logger()

logging.getLogger('databricks.labs.pytester').setLevel(logging.DEBUG)


@fixture
def debug_env_name():
    return "ucws"
