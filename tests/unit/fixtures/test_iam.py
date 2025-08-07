import sys
import warnings
from functools import partial
from unittest.mock import call

import pytest

from databricks.labs.pytester.fixtures.iam import make_acc_group, make_group, make_user, make_run_as, Group
from databricks.labs.pytester.fixtures.unwrap import call_stateful, CallContext


def test_make_user_no_args() -> None:
    ctx, user = call_stateful(make_user)
    assert ctx is not None
    assert user is not None
    ctx['ws'].users.create.assert_called_once()
    ctx['ws'].users.delete.assert_called_once()


def test_make_run_as_no_args() -> None:
    ctx, run_as = call_stateful(make_run_as)
    assert ctx is not None
    assert run_as is not None
    ctx['acc'].service_principals.create.assert_called_once()
    ctx['acc'].service_principals.delete.assert_called_once()


def _setup_groups_api(call_context: CallContext, *, client_fixture_name: str) -> CallContext:
    """Minimum mocking of the specific client so that when a group is created it is also visible via the list() method.
    This is required because the make_group and make_acc_group fixtures double-check after creating a group to ensure
    the group is visible."""
    mock_group = Group(id="an_id")
    call_context[client_fixture_name].groups.create.return_value = mock_group
    call_context[client_fixture_name].groups.list.return_value = [mock_group]
    return call_context


def test_make_group_no_args() -> None:
    ctx, group = call_stateful(make_group, call_context_setup=partial(_setup_groups_api, client_fixture_name="ws"))

    assert group is not None
    ctx['ws'].groups.create.assert_called_once()
    assert ctx['ws'].groups.get.call_args_list == [call("an_id"), call("an_id")]
    assert ctx['ws'].groups.list.call_args_list == [
        call(attributes="id", filter='id eq "an_id"'),
        call(attributes="id", filter='id eq "an_id"'),
    ]
    ctx['ws'].groups.delete.assert_called_once()


def test_make_acc_group_no_args() -> None:
    ctx, group = call_stateful(make_acc_group, call_context_setup=partial(_setup_groups_api, client_fixture_name="acc"))

    assert group is not None
    ctx['acc'].groups.create.assert_called_once()
    assert ctx['acc'].groups.get.call_args_list == [call("an_id"), call("an_id")]
    assert ctx['acc'].groups.list.call_args_list == [
        call(attributes="id", filter='id eq "an_id"'),
        call(attributes="id", filter='id eq "an_id"'),
    ]
    ctx['acc'].groups.delete.assert_called_once()


@pytest.mark.parametrize(
    "make_group_fixture, client_fixture_name",
    [(make_group, "ws"), (make_acc_group, "acc")],
)
def test_make_group_deprecated_arg(make_group_fixture, client_fixture_name) -> None:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        call_stateful(
            make_group_fixture,
            wait_for_provisioning=True,
            call_context_setup=partial(_setup_groups_api, client_fixture_name=client_fixture_name),
        )

        # Check that the expected warning was emitted and attributed to the caller.
        (the_warning, *other_warnings) = w
        assert not other_warnings
        assert issubclass(the_warning.category, DeprecationWarning)
        assert "wait_for_provisioning when making a group is deprecated" in str(the_warning.message)
        assert the_warning.filename == sys.modules[call_stateful.__module__].__file__
