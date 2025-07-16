"""Unwrapping pytest fixtures for unit testing."""

import inspect
from collections.abc import Callable, Generator
from typing import TypeVar
from unittest.mock import MagicMock, create_autospec

import pytest

from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

import databricks.labs.pytester.fixtures.plugin as P

T = TypeVar('T')


# Pytest fixtures are not supposed to be called directly, but historically we hack through the countermeasures.
# TODO: Investigate and fix this if possible, to avoid breakage in future pytest versions.
# Potential solution: use `pytest.FixtureRequest` & `request.getfixturevalue()` to access fixtures.
if pytest.version_tuple >= (8, 4):

    def call_fixture(fixture_fn: Callable[..., T], *args, **kwargs) -> T:
        if not hasattr(fixture_fn, "_get_wrapped_function"):
            raise ValueError(f'{fixture_fn} is not a pytest fixture')
        accessor = getattr(fixture_fn, "_get_wrapped_function")
        wrapped = accessor()
        return wrapped(*args, **kwargs)

else:
    # Older versions of pytest use a different mechanism to wrap fixtures.
    def call_fixture(fixture_fn: Callable[..., T], *args, **kwargs) -> T:
        if not hasattr(fixture_fn, '__pytest_wrapped__'):
            raise ValueError(f'{fixture_fn} is not a pytest fixture')
        wrapped = getattr(fixture_fn, '__pytest_wrapped__')
        if not hasattr(wrapped, 'obj'):
            raise ValueError(f'{fixture_fn} is not a pytest fixture')
        return wrapped.obj(*args, **kwargs)


class CallContext:
    def __init__(self):
        workspace_client = create_autospec(WorkspaceClient)
        workspace_client.current_user.me.return_value = iam.User(
            user_name="test-user",
            groups=[iam.ComplexValue(display="admins")],
        )
        workspace_client.config.host = "https://adb-12345679.10.azuredatabricks.net/"
        workspace_client.get_workspace_id.return_value = 123456789
        self._fixtures = {
            'sql_backend': MockBackend(),
            'ws': workspace_client,
            'make_random': self.make_random,
            'env_or_skip': self.env_or_skip,
            'watchdog_remove_after': '2024091313',
            'watchdog_purge_suffix': 'XXXXX',
        }

    def __getitem__(self, name: str):
        if name in self._fixtures:
            return self._fixtures[name]
        self._fixtures[name] = MagicMock()  # pylint: disable=obscure-mock
        return self._fixtures[name]

    def or_mock(self, name: str):
        if name in self._fixtures:
            return self._fixtures[name]
        self._fixtures[name] = MagicMock()  # pylint: disable=obscure-mock
        return self._fixtures[name]

    def __setitem__(self, key, value):
        self._fixtures[key] = value

    def __contains__(self, item):
        return item in self._fixtures

    @staticmethod
    def make_random(_=None):
        return 'RANDOM'

    @staticmethod
    def env_or_skip(name: str) -> str:
        return name

    def __str__(self):
        names = ', '.join(self._fixtures.keys())
        return f'CallContext<{names}>'


_GENERATORS = set[str]()


def call_stateful(
    some: Callable[..., Generator[Callable[..., T]]],
    call_context_setup: Callable[[CallContext], CallContext] = lambda x: x,
    **kwargs,
) -> tuple[CallContext, T]:
    # pylint: disable=too-complex
    ctx = CallContext()
    ctx = call_context_setup(ctx)
    drains = []

    if len(_GENERATORS) == 0:
        # discover all generator fixtures
        for name in P.__all__:
            fixture_fn = getattr(P, name)
            sig = inspect.signature(fixture_fn)
            if not hasattr(sig.return_annotation, '__origin__'):
                continue
            if sig.return_annotation.__origin__ != Generator:
                continue
            _GENERATORS.add(name)

    def _bfs_call_context(fn: Callable) -> Generator:
        sig = inspect.signature(fn)
        init = {}
        for param in sig.parameters.values():
            if param.name in _GENERATORS:
                upstream_fixture = getattr(P, param.name)
                init[param.name] = _bfs_call_context(upstream_fixture)
                continue
            init[param.name] = ctx.or_mock(param.name)
        yielding = call_fixture(fn, **init)
        ctx[fn.__name__] = next(yielding)
        drains.append(yielding)
        return ctx[fn.__name__]

    _bfs_call_context(some)
    result = ctx[some.__name__](**kwargs)

    for generator in reversed(drains):
        try:  # drain the generator and call cleanup
            next(generator)
        except StopIteration:
            pass

    return ctx, result
