from collections.abc import Generator, Callable

from pytest import fixture
from databricks.sdk.service._internal import Wait
from databricks.sdk.service.serving import (
    ServingEndpointDetailed,
    EndpointCoreConfigInput,
    ServedModelInput,
    ServedModelInputWorkloadSize,
    EndpointTag,
)
from databricks.sdk.service.ml import CreateExperimentResponse, ModelTag, GetModelResponse

from databricks.labs.pytester.fixtures.baseline import factory


@fixture
def make_experiment(
    ws,
    make_random,
    make_directory,
    log_workspace_link,
    watchdog_purge_suffix,
) -> Generator[CreateExperimentResponse, None, None]:
    """
    Returns a function to create Databricks Experiments and clean them up after the test.
    The function returns a `databricks.sdk.service.ml.CreateExperimentResponse` object.

    Keyword arguments:
    * `path` (str, optional): The path of the experiment. Defaults to `dummy-*` experiment in current user's home folder.
    * `experiment_name` (str, optional): The name of the experiment. Defaults to `dummy-*`.

    Usage:
    ```python
    from databricks.sdk.service.iam import PermissionLevel

    def test_experiments(make_group, make_experiment, make_experiment_permissions):
        group = make_group()
        experiment = make_experiment()
        make_experiment_permissions(
            object_id=experiment.experiment_id,
            permission_level=PermissionLevel.CAN_MANAGE,
            group_name=group.display_name,
        )
    ```
    """

    def create(
        *,
        path: str | None = None,
        experiment_name: str | None = None,
        **kwargs,
    ) -> CreateExperimentResponse:
        folder = make_directory(path=path)
        if experiment_name is None:
            # The purge suffix is needed here as well, just in case the path was supplied.
            experiment_name = f"dummy-{make_random(4)}-{watchdog_purge_suffix}"
        experiment = ws.experiments.create_experiment(name=f"{folder}/{experiment_name}", **kwargs)
        log_workspace_link(f'{experiment_name} experiment', f'ml/experiments/{experiment.experiment_id}', anchor=False)
        return experiment

    yield from factory("experiment", create, lambda item: ws.experiments.delete_experiment(item.experiment_id))


@fixture
def make_model(ws, make_random, watchdog_remove_after) -> Generator[Callable[..., GetModelResponse], None, None]:
    """
    Returns a function to create Databricks Models and clean them up after the test.
    The function returns a `databricks.sdk.service.ml.GetModelResponse` object.

    Keyword arguments:
    * `model_name` (str, optional): The name of the model. Defaults to `dummy-*`.

    Usage:
    ```python
    from databricks.sdk.service.iam import PermissionLevel

    def test_models(make_group, make_model, make_registered_model_permissions):
        group = make_group()
        model = make_model()
        make_registered_model_permissions(
            object_id=model.id,
            permission_level=PermissionLevel.CAN_MANAGE,
            group_name=group.display_name,
        )
    ```
    """

    def create(*, model_name: str | None = None, **kwargs) -> GetModelResponse:
        if model_name is None:
            model_name = f"dummy-{make_random(4)}"
        remove_after_tag = ModelTag(key="RemoveAfter", value=watchdog_remove_after)
        if 'tags' not in kwargs:
            kwargs["tags"] = [remove_after_tag]
        else:
            kwargs["tags"].append(remove_after_tag)
        created_model = ws.model_registry.create_model(model_name, **kwargs)
        model = ws.model_registry.get_model(created_model.registered_model.name)
        return model.registered_model_databricks

    yield from factory("model", create, lambda item: ws.model_registry.delete_model(item.id))


@fixture
def make_serving_endpoint(ws, make_random, make_model, watchdog_remove_after):
    """
    Returns a function to create Databricks Serving Endpoints and clean them up after the test.
    The function returns a `databricks.sdk.service.serving.ServingEndpointDetailed` object.

    Under the covers, this fixture also creates a model to serve on a small workload size.

    Usage:
    ```python
    def test_endpoints(make_group, make_serving_endpoint, make_serving_endpoint_permissions):
        group = make_group()
        endpoint = make_serving_endpoint()
        make_serving_endpoint_permissions(
            object_id=endpoint.response.id,
            permission_level=PermissionLevel.CAN_QUERY,
            group_name=group.display_name,
        )
    ```
    """

    def create() -> Wait[ServingEndpointDetailed]:
        endpoint_name = make_random(4)
        model = make_model()
        endpoint = ws.serving_endpoints.create(
            endpoint_name,
            EndpointCoreConfigInput(
                served_models=[
                    ServedModelInput(
                        model_name=model.name,
                        model_version="1",
                        scale_to_zero_enabled=True,
                        workload_size=ServedModelInputWorkloadSize.SMALL,
                    )
                ]
            ),
            tags=[EndpointTag(key="RemoveAfter", value=watchdog_remove_after)],
        )
        return endpoint

    def remove(endpoint_name: str):
        ws.serving_endpoints.delete(endpoint_name)

    yield from factory("Serving endpoint", create, remove)
