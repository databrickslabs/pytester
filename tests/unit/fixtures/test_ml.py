import pytest

from databricks.sdk.errors import InvalidParameterValue

from databricks.labs.pytester.fixtures.ml import make_experiment, make_model, make_serving_endpoint
from databricks.labs.pytester.fixtures.unwrap import CallContext, call_stateful


def test_make_experiment_no_args():
    ctx, experiment = call_stateful(make_experiment)
    assert ctx is not None
    assert experiment is not None


def test_make_model_no_args():
    ctx, model = call_stateful(make_model)
    assert ctx is not None
    assert model is not None


def test_make_serving_endpoint_no_args() -> None:
    ctx, serving_endpoint = call_stateful(make_serving_endpoint)
    assert ctx is not None
    assert serving_endpoint is not None


def test_make_serving_endpoint_sets_endpoint_name() -> None:
    _, serving_endpoint = call_stateful(make_serving_endpoint, endpoint_name="test")
    assert serving_endpoint.name == "test"


def test_make_serving_endpoint_sets_model_name() -> None:
    _, serving_endpoint = call_stateful(make_serving_endpoint, model_name="test")
    assert serving_endpoint.pending_config.served_models[0].model_name == "test"


@pytest.mark.parametrize("model_name", [None, "test"])
def test_make_serving_endpoint_sets_default_model_version_to_one(model_name: str | None) -> None:
    """The default model version should be '1' independent

    Independent of the model name, if the latest version cannot be retrieved.
    """

    def _setup_model_registry_api(call_context: CallContext) -> CallContext:
        """Set up the model registry api for unit testing."""
        call_context["ws"].model_registry.get_latest_versions.side_effect = InvalidParameterValue("test")
        return call_context

    _, serving_endpoint = call_stateful(
        make_serving_endpoint, model_name=model_name, call_context_setup=_setup_model_registry_api
    )
    assert serving_endpoint.pending_config.served_models[0].model_version == "1"
