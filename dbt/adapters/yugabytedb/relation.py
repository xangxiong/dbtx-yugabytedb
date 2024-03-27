from dataclasses import dataclass
from typing import Optional, Set, FrozenSet

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.context.providers import RuntimeConfigObject
from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.yugabytedb.relation_configs import (
    YugabytedbIndexConfig,
    YugabytedbIndexConfigChange,
    YugabytedbMaterializedViewConfig,
    YugabytedbMaterializedViewConfigChangeCollection,
    MAX_CHARACTERS_IN_IDENTIFIER,
)


@dataclass(frozen=True, eq=False, repr=False)
class YugabytedbRelation(BaseRelation):
    renameable_relations = frozenset(
        {
            RelationType.View,
            RelationType.Table,
            RelationType.MaterializedView,
        }
    )
    replaceable_relations = frozenset(
        {
            RelationType.View,
            RelationType.Table,
        }
    )

    def __post_init__(self):
        # Check for length of Yugabytedb table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > self.relation_max_name_length()
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    def relation_max_name_length(self):
        return MAX_CHARACTERS_IN_IDENTIFIER

    def get_materialized_view_config_change_collection(
        self, relation_results: RelationResults, runtime_config: RuntimeConfigObject
    ) -> Optional[YugabytedbMaterializedViewConfigChangeCollection]:
        config_change_collection = YugabytedbMaterializedViewConfigChangeCollection()

        existing_materialized_view = YugabytedbMaterializedViewConfig.from_relation_results(
            relation_results
        )
        new_materialized_view = YugabytedbMaterializedViewConfig.from_model_node(
            runtime_config.model
        )

        config_change_collection.indexes = self._get_index_config_changes(
            existing_materialized_view.indexes, new_materialized_view.indexes
        )

        # we return `None` instead of an empty `YugabytedbMaterializedViewConfigChangeCollection` object
        # so that it's easier and more extensible to check in the materialization:
        # `core/../materializations/materialized_view.sql` :
        #     {% if configuration_changes is none %}
        if config_change_collection.has_changes:
            return config_change_collection

    def _get_index_config_changes(
        self,
        existing_indexes: FrozenSet[YugabytedbIndexConfig],
        new_indexes: FrozenSet[YugabytedbIndexConfig],
    ) -> Set[YugabytedbIndexConfigChange]:
        """
        Get the index updates that will occur as a result of a new run

        There are four scenarios:

        1. Indexes are equal -> don't return these
        2. Index is new -> create these
        3. Index is old -> drop these
        4. Indexes are not equal -> drop old, create new -> two actions

        Returns: a set of index updates in the form {"action": "drop/create", "context": <IndexConfig>}
        """
        drop_changes = set(
            YugabytedbIndexConfigChange.from_dict(
                {"action": RelationConfigChangeAction.drop, "context": index}
            )
            for index in existing_indexes.difference(new_indexes)
        )
        create_changes = set(
            YugabytedbIndexConfigChange.from_dict(
                {"action": RelationConfigChangeAction.create, "context": index}
            )
            for index in new_indexes.difference(existing_indexes)
        )
        return set().union(drop_changes, create_changes)
