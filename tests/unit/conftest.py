import pytest

from databricks.sdk.config import Config

pytest_plugins = ['pytester']


@pytest.fixture
def no_config_host_metadata_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suspend host metadata lookups from the SDKs Config class.

    As of version 0.102.0 of the Databricks SDK, the `Config` class during __init__() makes a network call with a
    timeout of 5 minutes if the target host does not exist. For unit tests, it never does. This fixture therefore
    patches over that lookup to ensure it doesn't get in the way of our tests.
    """

    def _fake_resolve_host_metadata(_self) -> None:
        return

    monkeypatch.setattr(Config, "_resolve_host_metadata", _fake_resolve_host_metadata)
