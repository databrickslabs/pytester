# Python Testing for Databricks

<!-- TOC -->
* [Python Testing for Databricks](#python-testing-for-databricks)
  * [PyTest Fixtures](#pytest-fixtures)
    * [`ws` fixture](#ws-fixture)
    * [`debug_env` fixture](#debug_env-fixture)
    * [`debug_env_name` fixture](#debug_env_name-fixture)
    * [`env_or_skip` fixture](#env_or_skip-fixture)
    * [`make_random` fixture](#make_random-fixture)
    * [`make_instance_pool` fixture](#make_instance_pool-fixture)
    * [`make_job` fixture](#make_job-fixture)
    * [`make_cluster` fixture](#make_cluster-fixture)
    * [`make_cluster_policy` fixture](#make_cluster_policy-fixture)
    * [`make_group` fixture](#make_group-fixture)
    * [`make_user` fixture](#make_user-fixture)
    * [`make_notebook` fixture](#make_notebook-fixture)
    * [`make_directory` fixture](#make_directory-fixture)
    * [`make_repo` fixture](#make_repo-fixture)
    * [`make_secret_scope` fixture](#make_secret_scope-fixture)
    * [`make_secret_scope_acl` fixture](#make_secret_scope_acl-fixture)
    * [`make_udf` fixture](#make_udf-fixture)
    * [`make_catalog` fixture](#make_catalog-fixture)
    * [`make_schema` fixture](#make_schema-fixture)
    * [`make_table` fixture](#make_table-fixture)
    * [`product_info` fixture](#product_info-fixture)
    * [`sql_backend` fixture](#sql_backend-fixture)
    * [`sql_exec` fixture](#sql_exec-fixture)
    * [`sql_fetch_all` fixture](#sql_fetch_all-fixture)
    * [`workspace_library` fixture](#workspace_library-fixture)
* [Project Support](#project-support)
<!-- TOC -->

## PyTest Fixtures

<!-- FIXTURES -->
### `debug_env_name` fixture
Specify the name of the debug environment. By default, it is set to `.env`,
which will try to find a [file named `.env`](https://www.dotenv.org/docs/security/env)
in any of the parent directories of the current working directory and load
the environment variables from it via the [`debug_env` fixture](#debug_env-fixture).

Alternatively, if you are concerned of the
[risk of `.env` files getting checked into version control](https://thehackernews.com/2024/08/attackers-exploit-public-env-files-to.html),
we recommend using the `~/.databricks/debug-env.json` file to store different sets of environment variables.
The file cannot be checked into version control by design, because it is stored in the user's home directory.

This file is used for local debugging and integration tests in IDEs like PyCharm, VSCode, and IntelliJ IDEA
while developing Databricks Platform Automation Stack, which includes Databricks SDKs for Python, Go, and Java,
as well as Databricks Terraform Provider and Databricks CLI. This file enables multi-environment and multi-cloud
testing with a single set of integration tests.

The file is typically structured as follows:

```shell
$ cat ~/.databricks/debug-env.json
{
   "ws": {
     "CLOUD_ENV": "azure",
     "DATABRICKS_HOST": "....azuredatabricks.net",
     "DATABRICKS_CLUSTER_ID": "0708-200540-...",
     "DATABRICKS_WAREHOUSE_ID": "33aef...",
        ...
   },
   "acc": {
     "CLOUD_ENV": "aws",
     "DATABRICKS_HOST": "accounts.cloud.databricks.net",
     "DATABRICKS_CLIENT_ID": "....",
     "DATABRICKS_CLIENT_SECRET": "....",
     ...
   }
}
```

And you can load it in your `conftest.py` file as follows:

```python
@pytest.fixture
def debug_env_name():
    return "ws"
```

This will load the `ws` environment from the `~/.databricks/debug-env.json` file.

If any of the environment variables are not found, [`env_or_skip` fixture](#env_or_skip-fixture)
will gracefully skip the execution of tests.

See also [`debug_env`](#debug_env-fixture).


[[back to top](#python-testing-for-databricks)]

### `debug_env` fixture
Loads environment variables specified in [`debug_env_name` fixture](#debug_env_name-fixture) from a file
for local debugging in IDEs, otherwise allowing the tests to run with the default environment variables
specified in the CI/CD pipeline.

See also [`env_or_skip`](#env_or_skip-fixture), [`ws`](#ws-fixture), [`debug_env_name`](#debug_env_name-fixture).


[[back to top](#python-testing-for-databricks)]

### `env_or_skip` fixture
Fixture to get environment variables or skip tests.

It is extremely useful to skip tests if the required environment variables are not set.

In the following example, `test_something` would only run if the environment variable
`SOME_EXTERNAL_SERVICE_TOKEN` is set:

```python
def test_something(env_or_skip):
    token = env_or_skip("SOME_EXTERNAL_SERVICE_TOKEN")
    assert token is not None
```

See also [`make_udf`](#make_udf-fixture), [`sql_backend`](#sql_backend-fixture), [`debug_env`](#debug_env-fixture).


[[back to top](#python-testing-for-databricks)]

### `ws` fixture
Create and provide a Databricks WorkspaceClient object.

This fixture initializes a Databricks WorkspaceClient object, which can be used
to interact with the Databricks workspace API. The created instance of WorkspaceClient
is shared across all test functions within the test session.

See [detailed documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html) for the list
of environment variables that can be used to authenticate the WorkspaceClient.

In your test functions, include this fixture as an argument to use the WorkspaceClient:

```python
def test_workspace_operations(ws):
    clusters = ws.clusters.list_clusters()
    assert len(clusters) >= 0
```

See also [`make_catalog`](#make_catalog-fixture), [`make_cluster`](#make_cluster-fixture), [`make_cluster_policy`](#make_cluster_policy-fixture), [`make_directory`](#make_directory-fixture), [`make_group`](#make_group-fixture), [`make_instance_pool`](#make_instance_pool-fixture), [`make_job`](#make_job-fixture), [`make_notebook`](#make_notebook-fixture), [`make_repo`](#make_repo-fixture), [`make_schema`](#make_schema-fixture), [`make_secret_scope`](#make_secret_scope-fixture), [`make_secret_scope_acl`](#make_secret_scope_acl-fixture), [`make_table`](#make_table-fixture), [`make_udf`](#make_udf-fixture), [`make_user`](#make_user-fixture), [`sql_backend`](#sql_backend-fixture), [`workspace_library`](#workspace_library-fixture), [`debug_env`](#debug_env-fixture), [`product_info`](#product_info-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_random` fixture
Fixture to generate random strings.

This fixture provides a function to generate random strings of a specified length.
The generated strings are created using a character set consisting of uppercase letters,
lowercase letters, and digits.

To generate a random string with default length of 16 characters:

```python
random_string = make_random()
assert len(random_string) == 16
```

To generate a random string with a specified length:

```python
random_string = make_random(k=8)
assert len(random_string) == 8
```

See also [`make_catalog`](#make_catalog-fixture), [`make_cluster`](#make_cluster-fixture), [`make_cluster_policy`](#make_cluster_policy-fixture), [`make_directory`](#make_directory-fixture), [`make_group`](#make_group-fixture), [`make_instance_pool`](#make_instance_pool-fixture), [`make_job`](#make_job-fixture), [`make_notebook`](#make_notebook-fixture), [`make_repo`](#make_repo-fixture), [`make_schema`](#make_schema-fixture), [`make_secret_scope`](#make_secret_scope-fixture), [`make_table`](#make_table-fixture), [`make_udf`](#make_udf-fixture), [`make_user`](#make_user-fixture), [`workspace_library`](#workspace_library-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_instance_pool` fixture
Fixture to manage Databricks instance pools.

This fixture provides a function to manage Databricks instance pools using the provided workspace (ws).
Instance pools can be created with specified configurations, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks instance pools.

Usage Example:
--------------
To manage Databricks instance pools using the make_instance_pool fixture:

.. code-block:: python

    def test_instance_pool_management(make_instance_pool):
        instance_pool_info = make_instance_pool(instance_pool_name="my-pool")
        assert instance_pool_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_job` fixture
Fixture to manage Databricks jobs.

This fixture provides a function to manage Databricks jobs using the provided workspace (ws).
Jobs can be created with specified configurations, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.
make_notebook : function
    The make_notebook fixture to create a notebook path.

Returns:
--------
function:
    A function to manage Databricks jobs.

Usage Example:
--------------
To manage Databricks jobs using the make_job fixture:

.. code-block:: python

    def test_job_management(make_job):
        job_info = make_job(name="my-job")
        assert job_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture), [`make_notebook`](#make_notebook-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_cluster` fixture
Fixture to manage Databricks clusters.

This fixture provides a function to manage Databricks clusters using the provided workspace (ws).
Clusters can be created with specified configurations, and they will be permanently deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks clusters.

Usage Example:
--------------
To manage Databricks clusters using the make_cluster fixture:

.. code-block:: python

    def test_cluster_management(make_cluster):
        cluster_info = make_cluster(cluster_name="my-cluster", single_node=True)
        assert cluster_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_cluster_policy` fixture
Fixture to manage Databricks cluster policies.

This fixture provides a function to manage Databricks cluster policies using the provided workspace (ws).
Cluster policies can be created with a specified name and definition, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks cluster policies.

Usage Example:
--------------
To manage Databricks cluster policies using the make_cluster_policy fixture:

.. code-block:: python

    def test_cluster_policy_management(make_cluster_policy):
        policy_info = make_cluster_policy(name="my-policy")
        assert policy_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_group` fixture
Fixture to manage Databricks workspace groups.

This fixture provides a function to manage Databricks workspace groups using the provided workspace (ws).
Groups can be created with specified members and roles, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks workspace groups.

Usage Example:
--------------
To manage Databricks workspace groups using the make_group fixture:

.. code-block:: python

    def test_group_management(make_group):
        group_info = make_group(members=["user@example.com"], roles=["viewer"])
        assert group_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_user` fixture
Fixture to manage Databricks workspace users.

This fixture provides a function to manage Databricks workspace users using the provided workspace (ws).
Users can be created with a generated user name, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks workspace users.

Usage Example:
--------------
To manage Databricks workspace users using the make_user fixture:

.. code-block:: python

    def test_user_management(make_user):
        user_info = make_user()
        assert user_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_notebook` fixture
Fixture to manage Databricks notebooks.

This fixture provides a function to manage Databricks notebooks using the provided workspace (ws).
Notebooks can be created with a specified path and content, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks notebooks.

Usage Example:
--------------
To manage Databricks notebooks using the make_notebook fixture:

.. code-block:: python

    def test_notebook_management(make_notebook):
        notebook_path = make_notebook()
        assert notebook_path.startswith("/Users/") and notebook_path.endswith(".py")

See also [`make_job`](#make_job-fixture), [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_directory` fixture
Fixture to manage Databricks directories.

This fixture provides a function to manage Databricks directories using the provided workspace (ws).
Directories can be created with a specified path, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks directories.

Usage Example:
--------------
To manage Databricks directories using the make_directory fixture:

.. code-block:: python

    def test_directory_management(make_directory):
        directory_path = make_directory()
        assert directory_path.startswith("/Users/") and not directory_path.endswith(".py")

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_repo` fixture
Fixture to manage Databricks repos.

This fixture provides a function to manage Databricks repos using the provided workspace (ws).
Repos can be created with a specified URL, provider, and path, and they will be deleted after the test is complete.

Parameters:
-----------
ws : WorkspaceClient
    A Databricks WorkspaceClient instance.
make_random : function
    The make_random fixture to generate unique names.

Returns:
--------
function:
    A function to manage Databricks repos.

Usage Example:
--------------
To manage Databricks repos using the make_repo fixture:

.. code-block:: python

    def test_repo_management(make_repo):
        repo_info = make_repo()
        assert repo_info is not None

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_secret_scope` fixture
This fixture provides a function to create secret scopes. The created secret scope will be
deleted after the test is complete. Returns the name of the secret scope.

To create a secret scope and use it within a test function:

```python
def test_secret_scope_creation(make_secret_scope):
    secret_scope_name = make_secret_scope()
    assert secret_scope_name.startswith("dummy-")
```

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_secret_scope_acl` fixture
This fixture provides a function to manage access control lists (ACLs) for secret scopes.
ACLs define permissions for principals (users or groups) on specific secret scopes.

Arguments:
- `scope`: The name of the secret scope.
- `principal`: The name of the principal (user or group).
- `permission`: The permission level for the principal on the secret scope.

Returns a tuple containing the secret scope name and the principal name.

To manage secret scope ACLs using the make_secret_scope_acl fixture:

```python
from databricks.sdk.service.workspace import AclPermission

def test_secret_scope_acl_management(make_user, make_secret_scope, make_secret_scope_acl):
    scope_name = make_secret_scope()
    principal_name = make_user().display_name
    permission = AclPermission.READ

    acl_info = make_secret_scope_acl(
        scope=scope_name,
        principal=principal_name,
        permission=permission,
    )
    assert acl_info == (scope_name, principal_name)
```

See also [`ws`](#ws-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_udf` fixture
_No description yet._

See also [`ws`](#ws-fixture), [`env_or_skip`](#env_or_skip-fixture), [`sql_backend`](#sql_backend-fixture), [`make_schema`](#make_schema-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_catalog` fixture
_No description yet._

See also [`ws`](#ws-fixture), [`sql_backend`](#sql_backend-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_schema` fixture
_No description yet._

See also [`make_table`](#make_table-fixture), [`make_udf`](#make_udf-fixture), [`ws`](#ws-fixture), [`sql_backend`](#sql_backend-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `make_table` fixture
_No description yet._

See also [`ws`](#ws-fixture), [`sql_backend`](#sql_backend-fixture), [`make_schema`](#make_schema-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

### `product_info` fixture
_No description yet._

See also [`ws`](#ws-fixture).


[[back to top](#python-testing-for-databricks)]

### `sql_backend` fixture
te and provide a SQL backend for executing statements.

Requires the environment variable `DATABRICKS_WAREHOUSE_ID` to be set.

See also [`make_catalog`](#make_catalog-fixture), [`make_schema`](#make_schema-fixture), [`make_table`](#make_table-fixture), [`make_udf`](#make_udf-fixture), [`sql_exec`](#sql_exec-fixture), [`sql_fetch_all`](#sql_fetch_all-fixture), [`ws`](#ws-fixture), [`env_or_skip`](#env_or_skip-fixture).


[[back to top](#python-testing-for-databricks)]

### `sql_exec` fixture
ute SQL statement and don't return any results.

See also [`sql_backend`](#sql_backend-fixture).


[[back to top](#python-testing-for-databricks)]

### `sql_fetch_all` fixture
h all rows from a SQL statement.

See also [`sql_backend`](#sql_backend-fixture).


[[back to top](#python-testing-for-databricks)]

### `workspace_library` fixture
_No description yet._

See also [`ws`](#ws-fixture), [`make_random`](#make_random-fixture).


[[back to top](#python-testing-for-databricks)]

<!-- END FIXTURES -->

# Project Support

Please note that this project is provided for your exploration only and is not 
formally supported by Databricks with Service Level Agreements (SLAs). They are 
provided AS-IS, and we do not make any guarantees of any kind. Please do not 
submit a support ticket relating to any issues arising from the use of this project.

Any issues discovered through the use of this project should be filed as GitHub 
[Issues on this repository](https://github.com/databrickslabs/pytester/issues). 
They will be reviewed as time permits, but no formal SLAs for support exist.
