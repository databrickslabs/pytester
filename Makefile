all: clean lint fmt test coverage

# Ensure that all uv commands don't automatically update the lock file: instead they use the locked dependencies.
export UV_FROZEN := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 4 --timeout 30 --durations 20

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

lint:
	$(UV_RUN) black --check . --extend-exclude 'tests/unit/source_code/samples/'
	$(UV_RUN) ruff check .
	$(UV_RUN) mypy --exclude 'tests/resources/*' --exclude dist .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

fmt:
	$(UV_RUN) python scripts/gen-readme.py
	$(UV_RUN) black . --extend-exclude 'tests/unit/source_code/samples/'
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) mypy --disable-error-code 'annotation-unchecked' --disable-error-code import-untyped --exclude 'tests/resources/*' --exclude dist .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

test:
	$(UV_TEST) --cov src --cov-report=xml tests/unit

integration:
	$(UV_TEST) --cov src --cov-report=xml tests/integration

coverage:
	$(UV_TEST) --cov src --cov-report=html tests/unit
	open htmlcov/index.html

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

lock-dependencies: UV_FROZEN := 0
lock-dependencies:
	uv lock --upgrade
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	    uv pip compile --upgrade --generate-hashes --universal --no-header --quiet - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt
	@perl -pi \
		-e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g;' \
		-e 's|url = "https://[^"]*/packages/([^"]*)"|url = "https://files.pythonhosted.org/packages/$$1"|g;' \
		uv.lock
	@printf 'Stripped registry references from uv.lock.\n'

.DEFAULT: all
.PHONY: all clean dev lint fmt test integration coverage build lock-dependencies
