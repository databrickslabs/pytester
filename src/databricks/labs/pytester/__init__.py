from databricks.sdk.useragent import with_extra

from databricks.labs.pytester.__about__ import __version__

with_extra("pytester", __version__)
