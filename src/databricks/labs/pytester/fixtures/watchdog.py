from datetime import timedelta, datetime, timezone
from pytest import fixture

TEST_RESOURCE_PURGE_TIMEOUT = timedelta(hours=1)


@fixture
def watchdog_remove_after() -> str:
    """
    Purge time for test objects, representing the (UTC-based) hour from which objects may be purged.
    """
    # Note: this code is duplicated in the workflow installer (WorkflowsDeployment) so that it can avoid the
    # transitive pytest deployment from this module.
    now = datetime.now(timezone.utc)
    purge_deadline = now + TEST_RESOURCE_PURGE_TIMEOUT
    # Round UP to the next hour boundary: that is when resources will be deleted.
    purge_hour = purge_deadline + (datetime.min.replace(tzinfo=timezone.utc) - purge_deadline) % timedelta(hours=1)
    return purge_hour.strftime("%Y%m%d%H")


@fixture
def watchdog_purge_suffix(watchdog_remove_after) -> str:
    """
    HEX-encoded purge time suffix for test objects.
    """
    return f'ra{int(watchdog_remove_after):x}'
