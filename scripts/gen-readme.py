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


@dataclass
class Fixture:
    name: str
    description: str
    upstreams: list[str]

    @staticmethod
    def ref(name: str) -> str:
        anchor = name.replace("_", "-")
        return f"[`{name}`](#{anchor}-fixture)"

    def doc(self) -> str:
        return "\n".join(
            [
                f"### `{self.name}` fixture",
                self.description if self.description else "_No description yet._",
                "",
                f"This fixture is built on top of: {', '.join([self.ref(up) for up in self.upstreams])}",
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


def discover_fixtures():
    fixtures = []
    for fixture in P.__all__:
        fn = getattr(P, fixture)
        upstreams = []
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            upstreams.append(param.name)
        fx = Fixture(
            name=fixture,
            description=fn.__doc__,
            upstreams=upstreams,
        )
        fixtures.append(fx)
    return fixtures


if __name__ == '__main__':
    main()
