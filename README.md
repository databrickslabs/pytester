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
### `ws` fixture

    Create and provide a Databricks WorkspaceClient object.

    This fixture initializes a Databricks WorkspaceClient object, which can be used
    to interact with the Databricks workspace API. The created instance of WorkspaceClient
    is shared across all test functions within the test session.

    See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html

    Returns:
    --------
    databricks.sdk.WorkspaceClient:
        An instance of WorkspaceClient for interacting with the Databricks Workspace APIs.

    Usage:
    ------
    In your test functions, include this fixture as an argument to use the WorkspaceClient:

    .. code-block:: python

        def test_workspace_operations(ws):
            clusters = ws.clusters.list_clusters()
            assert len(clusters) >= 0
    

This fixture is built on top of: [`debug_env`](#debug-env-fixture), [`product_info`](#product-info-fixture)


[[back to top](#python-testing-for-databricks)]

### `debug_env` fixture
_No description yet._

This fixture is built on top of: [`monkeypatch`](#monkeypatch-fixture), [`debug_env_name`](#debug-env-name-fixture)


[[back to top](#python-testing-for-databricks)]

### `debug_env_name` fixture
_No description yet._

This fixture is built on top of: 


[[back to top](#python-testing-for-databricks)]

### `env_or_skip` fixture
_No description yet._

This fixture is built on top of: [`debug_env`](#debug-env-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_random` fixture

    Fixture to generate random strings.

    This fixture provides a function to generate random strings of a specified length.
    The generated strings are created using a character set consisting of uppercase letters,
    lowercase letters, and digits.

    Returns:
    --------
    function:
        A function to generate random strings.

    Usage Example:
    --------------
    To generate a random string with default length of 16 characters:

    .. code-block:: python

       random_string = make_random()
       assert len(random_string) == 16

    To generate a random string with a specified length:

    .. code-block:: python

       random_string = make_random(k=8)
       assert len(random_string) == 8
    

This fixture is built on top of: 


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture), [`make_notebook`](#make-notebook-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


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
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_secret_scope` fixture

    Fixture to create secret scopes.

    This fixture provides a function to create secret scopes using the provided workspace (ws)
    and the make_random function for generating unique names. The created secret scope will be
    deleted after the test is complete.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.
    make_random : function
        The make_random fixture to generate unique names.

    Returns:
    --------
    function:
        A function to create secret scopes.

    Usage Example:
    --------------
    To create a secret scope and use it within a test function:

    .. code-block:: python

        def test_secret_scope_creation(make_secret_scope):
            secret_scope_name = make_secret_scope()
            assert secret_scope_name.startswith("sdk-")
    

This fixture is built on top of: [`ws`](#ws-fixture), [`make_random`](#make-random-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_secret_scope_acl` fixture

    Fixture to manage secret scope access control lists (ACLs).

    This fixture provides a function to manage access control lists (ACLs) for secret scopes
    using the provided workspace (ws). ACLs define permissions for principals (users or groups)
    on specific secret scopes.

    Parameters:
    -----------
    ws : WorkspaceClient
        A Databricks WorkspaceClient instance.

    Returns:
    --------
    function:
        A function to manage secret scope ACLs.

    Usage Example:
    --------------
    To manage secret scope ACLs using the make_secret_scope_acl fixture:

    .. code-block:: python

        def test_secret_scope_acl_management(make_secret_scope_acl):
            scope_name = "my_secret_scope"
            principal_name = "user@example.com"
            permission = workspace.AclPermission.READ

            acl_info = make_secret_scope_acl(scope=scope_name, principal=principal_name, permission=permission)
            assert acl_info == (scope_name, principal_name)
    

This fixture is built on top of: [`ws`](#ws-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_udf` fixture
_No description yet._

This fixture is built on top of: [`ws`](#ws-fixture), [`env_or_skip`](#env-or-skip-fixture), [`sql_backend`](#sql-backend-fixture), [`make_schema`](#make-schema-fixture), [`make_random`](#make-random-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_catalog` fixture
_No description yet._

This fixture is built on top of: [`ws`](#ws-fixture), [`sql_backend`](#sql-backend-fixture), [`make_random`](#make-random-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_schema` fixture
_No description yet._

This fixture is built on top of: [`ws`](#ws-fixture), [`sql_backend`](#sql-backend-fixture), [`make_random`](#make-random-fixture)


[[back to top](#python-testing-for-databricks)]

### `make_table` fixture
_No description yet._

This fixture is built on top of: [`ws`](#ws-fixture), [`sql_backend`](#sql-backend-fixture), [`make_schema`](#make-schema-fixture), [`make_random`](#make-random-fixture)


[[back to top](#python-testing-for-databricks)]

### `product_info` fixture
_No description yet._

This fixture is built on top of: 


[[back to top](#python-testing-for-databricks)]

### `sql_backend` fixture
Create and provide a SQL backend for executing statements.

This fixture is built on top of: [`ws`](#ws-fixture), [`env_or_skip`](#env-or-skip-fixture)


[[back to top](#python-testing-for-databricks)]

### `sql_exec` fixture
Execute SQL statement and don't return any results.

This fixture is built on top of: [`sql_backend`](#sql-backend-fixture)


[[back to top](#python-testing-for-databricks)]

### `sql_fetch_all` fixture
Fetch all rows from a SQL statement.

This fixture is built on top of: [`sql_backend`](#sql-backend-fixture)


[[back to top](#python-testing-for-databricks)]

### `workspace_library` fixture
_No description yet._

This fixture is built on top of: [`ws`](#ws-fixture), [`fresh_local_wheel_file`](#fresh-local-wheel-file-fixture), [`make_random`](#make-random-fixture)


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
