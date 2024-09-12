from pytest import fixture

from databricks.labs.pytester.fixtures.baseline import factory, get_purge_suffix


@fixture
def make_experiment(ws, make_random, make_directory, log_workspace_link):
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
    ):
        folder = make_directory(path=path)
        if experiment_name is None:
            # The purge suffix is needed here as well, just in case the path was supplied.
            experiment_name = f"dummy-{make_random(4)}-{get_purge_suffix()}"
        experiment = ws.experiments.create_experiment(name=f"{folder}/{experiment_name}", **kwargs)
        log_workspace_link(f'{experiment_name} experiment', f'ml/experiments/{experiment.experiment_id}', anchor=False)
        return experiment

    yield from factory("experiment", create, lambda item: ws.experiments.delete_experiment(item.experiment_id))
