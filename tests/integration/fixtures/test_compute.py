import logging

logger = logging.getLogger(__name__)


def test_cluster_policy(make_cluster_policy):
    logger.info(f"created {make_cluster_policy()}")


def test_cluster(make_cluster):
    logger.info(f"created {make_cluster(single_node=True)}")


def test_instance_pool(make_instance_pool):
    logger.info(f"created {make_instance_pool()}")
