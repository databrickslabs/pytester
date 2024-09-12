import logging
import os


logger = logging.getLogger(__name__)


def test_cluster(make_cluster):
    logger.info(f"created {make_cluster(single_node=True, instance_pool_id=os.environ['TEST_INSTANCE_POOL_ID'])}")


def test_job(make_job):
    logger.info(f"created {make_job()}")


def test_pipeline(make_pipeline):
    logger.info(f"created {make_pipeline()}")
