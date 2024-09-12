from databricks.sdk.service.iam import PermissionLevel


def test_experiments(make_group, make_experiment, make_experiment_permissions):
    group = make_group()
    experiment = make_experiment()
    make_experiment_permissions(
        object_id=experiment.experiment_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group.display_name,
    )
