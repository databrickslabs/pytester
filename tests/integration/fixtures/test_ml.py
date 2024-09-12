from databricks.sdk.service.iam import PermissionLevel


def test_experiments(make_group, make_experiment, make_experiment_permissions):
    group = make_group()
    experiment = make_experiment()
    make_experiment_permissions(
        object_id=experiment.experiment_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group.display_name,
    )


def test_models(make_group, make_model, make_registered_model_permissions):
    group = make_group()
    model = make_model()
    make_registered_model_permissions(
        object_id=model.id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group.display_name,
    )


def test_endpoints(make_group, make_serving_endpoint, make_serving_endpoint_permissions):
    group = make_group()
    endpoint = make_serving_endpoint()
    make_serving_endpoint_permissions(
        object_id=endpoint.response.id,
        permission_level=PermissionLevel.CAN_QUERY,
        group_name=group.display_name,
    )
