import logging


logger = logging.getLogger(__name__)


def test_pipeline(make_pipeline):
    logger.info(f"created {make_pipeline()}")
