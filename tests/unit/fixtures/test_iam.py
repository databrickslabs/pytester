import warnings
import sys
from unittest.mock import call, create_autospec

import pytest
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.pytester.fixtures.iam import make_user, make_group, make_acc_group, Group
from databricks.labs.pytester.fixtures.unwrap import call_stateful, fixtures


def test_make_user_no_args() -> None:
    ctx, user = call_stateful(make_user)
    assert ctx is not None
    assert user is not None
    ctx['ws'].users.create.assert_called_once()
    ctx['ws'].users.delete.assert_called_once()


def test_make_group_no_args() -> None:
    ws = create_autospec(WorkspaceClient)
    mock_group = Group(id="an_id")
    ws.groups.create.return_value = mock_group
    ws.groups.list.return_value = [mock_group]

    with fixtures(ws=ws):
        ctx, group = call_stateful(make_group)

    assert ctx is not None and ctx['ws'] is ws
    assert group is mock_group

    ws.groups.create.assert_called_once()
    assert ws.groups.get.call_args_list == [call("an_id"), call("an_id")]
    assert ws.groups.list.call_args_list == [
        call(attributes="id", filter='id eq "an_id"'),
        call(attributes="id", filter='id eq "an_id"'),
    ]
    ws.groups.delete.assert_called_once()
    ctx['ws'].groups.delete.assert_called_once()


def test_make_acc_group_no_args() -> None:
    acc = create_autospec(AccountClient)
    mock_group = Group(id="an_id")
    acc.groups.create.return_value = mock_group
    acc.groups.list.return_value = [mock_group]

    with fixtures(acc=acc):
        ctx, group = call_stateful(make_acc_group)

    assert ctx is not None and ctx['acc'] is acc
    assert group is mock_group

    acc.groups.create.assert_called_once()
    assert acc.groups.get.call_args_list == [call("an_id"), call("an_id")]
    assert acc.groups.list.call_args_list == [
        call(attributes="id", filter='id eq "an_id"'),
        call(attributes="id", filter='id eq "an_id"'),
    ]
    acc.groups.delete.assert_called_once()


@pytest.mark.parametrize(
    "make_group_fixture, client_fixture_name, client_class",
    [(make_group, "ws", WorkspaceClient), (make_acc_group, "acc", AccountClient)],
)
def test_make_group_deprecated_arg(make_group_fixture, client_fixture_name, client_class) -> None:
    client = create_autospec(client_class)
    mock_group = Group(id="an_id")
    client.groups.create.return_value = mock_group
    client.groups.list.return_value = [mock_group]

    with fixtures(**{client_fixture_name: client}), warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        call_stateful(make_group_fixture, wait_for_provisioning=True)

        # Check that the expected warning was emitted and attributed to the caller.
        (the_warning,) = w
        assert issubclass(the_warning.category, DeprecationWarning)
        assert "wait_for_provisioning when making a group is deprecated" in str(the_warning.message)
        assert the_warning.filename == sys.modules[call_stateful.__module__].__file__
