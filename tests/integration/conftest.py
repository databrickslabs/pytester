import logging

from pytest import fixture
from databricks.labs.blueprint.logger import install_logger

from databricks.labs.pytester.__about__ import __version__

install_logger()

logging.getLogger('databricks.labs.pytester').setLevel(logging.DEBUG)


@fixture
def debug_env_name():
    return "ucws"


@fixture
def product_info():
    return 'pytester', __version__
