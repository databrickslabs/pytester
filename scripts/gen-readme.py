import collections
import inspect
import re
from dataclasses import dataclass
from pathlib import Path

import databricks.labs.pytester.fixtures.plugin as P

BACK_TO_TOP = "\n[[back to top](#python-testing-for-databricks)]\n"


def main():
    out = []
    for fixture in discover_fixtures():
        out.append(fixture.doc())
    overwrite_readme('FIXTURES', "\n".join(out))


DATACLASS_RE = re.compile(r"`databricks.sdk.service.(\w+).(\w+)`", re.DOTALL)
DATACLASS_DOC = (
    r'[`\2`](https://databricks-sdk-py.readthedocs.io/en/latest/dbdataclasses/\1.html#databricks.sdk.service.\1.\2)'
)


@dataclass
class Fixture:
    name: str
    description: str
    see_also: list[str]

    @staticmethod
    def ref(name: str) -> str:
        return f"[`{name}`](#{name}-fixture)"

    def usage(self) -> str:
        lines = "\n".join(_[4:] for _ in self.description.split("\n"))
        # replace all occurrences of `databricks.sdk.service.*.*` with a link
        lines = DATACLASS_RE.sub(DATACLASS_DOC, lines)
        return lines.strip()

    def doc(self) -> str:
        return "\n".join(
            [
                f"### `{self.name}` fixture",
                self.usage() if self.description else "_No description yet._",
                "",
                f"See also {', '.join([self.ref(up) for up in self.see_also])}.",
                "",
                BACK_TO_TOP,
            ]
        )


def overwrite_readme(part, docs):
    docs = f"<!-- {part} -->\n{docs}\n<!-- END {part} -->"
    readme_file = Path(__file__).parent.parent.joinpath("README.md")
    with readme_file.open("r") as f:
        pattern = rf"<!-- {part} -->\n(.*)\n<!-- END {part} -->"
        new_readme = re.sub(pattern, docs, f.read(), 0, re.MULTILINE | re.DOTALL)
    with readme_file.open("w") as f:
        f.write(new_readme)


def discover_fixtures() -> list[Fixture]:
    fixtures: list[Fixture] = []
    see_also = collections.defaultdict(set)
    idx: dict[str, int] = {}
    for fixture in P.__all__:
        fn = getattr(P, fixture)
        upstreams = []
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            if param.name in {'fresh_local_wheel_file', 'monkeypatch', 'log_workspace_link'}:
                continue
            upstreams.append(param.name)
            see_also[param.name].add(fixture)
        fx = Fixture(
            name=fixture,
            description=fn.__doc__,
            see_also=upstreams,
        )
        idx[fixture] = len(fixtures)
        fixtures.append(fx)
    for fixture, other in see_also.items():
        fx = fixtures[idx[fixture]]
        fx.see_also = sorted(other) + fx.see_also
    return fixtures


if __name__ == '__main__':
    main()
