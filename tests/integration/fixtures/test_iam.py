from databricks.sdk.service.workspace import ObjectType


def test_new_user(make_user, ws):
    new_user = make_user()
    home_dir = ws.workspace.get_status(f"/Users/{new_user.user_name}")
    assert home_dir.object_type == ObjectType.DIRECTORY


def test_new_group(make_group, make_user, ws):
    user = make_user()
    group = make_group(members=[user.id])
    loaded = ws.groups.get(group.id)
    assert group.display_name == loaded.display_name
    assert group.members == loaded.members
