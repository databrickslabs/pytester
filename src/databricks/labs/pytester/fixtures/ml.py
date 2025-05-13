import logging
from collections.abc import Callable, Generator
from unittest.mock import Mock

from pytest import fixture
from databricks.sdk.errors import BadRequest
from databricks.sdk.service._internal import Wait
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    EndpointPendingConfig,
    EndpointTag,
    ServedModelInput,
    ServedModelOutput,
    ServingEndpointDetailed,
)
from databricks.sdk.service.ml import CreateExperimentResponse, ModelDatabricks, ModelTag

from databricks.labs.pytester.fixtures.baseline import factory


logger = logging.getLogger(__name__)


@fixture
def make_experiment(
    ws,
    make_random,
    make_directory,
    log_workspace_link,
    watchdog_purge_suffix,
) -> Generator[Callable[..., CreateExperimentResponse], None, None]:
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
def make_model(ws, make_random, watchdog_remove_after) -> Generator[Callable[..., ModelDatabricks], None, None]:
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

    def create(*, model_name: str | None = None, **kwargs) -> ModelDatabricks:
        if model_name is None:
            model_name = f"dummy-{make_random(4)}"
        remove_after_tag = ModelTag(key="RemoveAfter", value=watchdog_remove_after)
        if 'tags' not in kwargs:
            kwargs["tags"] = [remove_after_tag]
        else:
            kwargs["tags"].append(remove_after_tag)
        created_model = ws.model_registry.create_model(model_name, **kwargs)
        model = ws.model_registry.get_model(created_model.registered_model.name)
        assert model.registered_model_databricks is not None
        return model.registered_model_databricks

    yield from factory("model", create, lambda item: ws.model_registry.delete_model(item.id))


@fixture
def make_serving_endpoint(ws, make_random, watchdog_remove_after):
    """
    Returns a function to create Databricks Serving Endpoints and clean them up after the test.
    The function returns a `databricks.sdk.service.serving.ServingEndpointDetailed` object.

    Under the covers, this fixture also creates a model to serve on a small workload size.

    Keyword arguments:
    * `endpoint_name` (str, optional): The name of the endpoint. Defaults to `dummy-*`.
    * `model_name` (str, optional): The name of the model to serve on the endpoint.
        Defaults to system model `system.ai.llama_v3_2_1b_instruct`.
    * `model_version` (str, optional): The model version to serve. If None, tries to get the latest version for
        workspace local models. Otherwise, defaults to version `1`.

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

    def create(
        *,
        endpoint_name: str | None = None,
        model_name: str | None = None,
        model_version: str | None = None,
    ) -> Wait[ServingEndpointDetailed]:
        endpoint_name = endpoint_name or make_random(4)
        model_name = model_name or "system.ai.llama_v3_2_1b_instruct"
        if not model_version and "." not in model_name:  # The period in the name signals it is NOT workspace local
            try:
                model_version = ws.model_registry.get_latest_versions(model_name).version
            except BadRequest as e:
                logger.warning(
                    f"Cannot get latest version for model: {model_name}. Fallback to version '1'.", exc_info=e
                )
        model_version = model_version or "1"
        tags = [EndpointTag(key="RemoveAfter", value=watchdog_remove_after)]
        served_model_input = ServedModelInput(
            model_name=model_name,
            model_version=model_version,
            scale_to_zero_enabled=True,
            workload_size="Small",
        )
        endpoint = ws.serving_endpoints.create(
            endpoint_name,
            config=EndpointCoreConfigInput(served_models=[served_model_input]),
            tags=tags,
        )
        if isinstance(endpoint, Mock):  # For testing
            served_model_output = ServedModelOutput(
                model_name=model_name,
                model_version=model_version,
                scale_to_zero_enabled=True,
                workload_size="Small",
            )
            endpoint = ServingEndpointDetailed(
                name=endpoint_name,
                pending_config=EndpointPendingConfig(served_models=[served_model_output]),
                tags=tags,
            )
        return endpoint

    def remove(endpoint: ServingEndpointDetailed) -> None:
        if endpoint.name:
            ws.serving_endpoints.delete(endpoint.name)

    yield from factory("Serving endpoint", create, remove)


@fixture
def make_feature_table(ws, make_random):
    def create():
        feature_table_name = make_random(6) + "." + make_random(6)
        table = ws.api_client.do(
            "POST",
            "/api/2.0/feature-store/feature-tables/create",
            body={"name": feature_table_name, "primary_keys": [{"name": "pk", "data_type": "string"}]},
        )
        return table['feature_table']

    def remove(table: dict):
        ws.api_client.do("DELETE", "/api/2.0/feature-store/feature-tables/delete", body={"name": table["name"]})

    yield from factory("Feature table", create, remove)
