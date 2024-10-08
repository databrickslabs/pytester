from databricks.labs.pytester.fixtures.ml import make_experiment, make_model, make_serving_endpoint
from databricks.labs.pytester.fixtures.unwrap import call_stateful


def test_make_experiment_no_args():
    ctx, experiment = call_stateful(make_experiment)
    assert ctx is not None
    assert experiment is not None


def test_make_model_no_args():
    ctx, model = call_stateful(make_model)
    assert ctx is not None
    assert model is not None


def test_make_serving_endpoint_no_args():
    ctx, serving_endpoint = call_stateful(make_serving_endpoint)
    assert ctx is not None
    assert serving_endpoint is not None
