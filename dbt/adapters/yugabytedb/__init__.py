# these are mostly just exports, #noqa them so flake8 will be happy
from dbt.adapters.yugabytedb.connections import YugabytedbConnectionManager  # noqa
from dbt.adapters.yugabytedb.connections import YugabytedbCredentials
from dbt.adapters.yugabytedb.column import YugabytedbColumn  # noqa
from dbt.adapters.yugabytedb.relation import YugabytedbRelation  # noqa: F401
from dbt.adapters.yugabytedb.impl import YugabytedbAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import yugabytedb

Plugin = AdapterPlugin(
    adapter=YugabytedbAdapter, credentials=YugabytedbCredentials, include_path=yugabytedb.PACKAGE_PATH
)
