from databricks.sdk.core import with_user_agent_extra

from databricks.labs.pytester.__about__ import __version__

with_user_agent_extra("pytester", __version__)
