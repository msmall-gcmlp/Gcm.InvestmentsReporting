from gcm.inv.utils.misc.table_cache_base import Singleton
from ......core.entity_handler import (
    HierarchyHandler,
    EntityDomainTypes,
    EntityStandardNames,
    List,
)
from functools import cached_property
import pandas as pd
from .gets.get_cfs import get_cfs
from .gets.get_positions import get_positions
from .gets.get_position_map import get_position_map


class TrackRecordHandler(object):
    def __init__(self, manager_name: str):
        self.manager_name = manager_name

    @cached_property
    def manager_hierarchy_structure(self) -> HierarchyHandler:
        return HierarchyHandler(
            EntityDomainTypes.InvestmentManager, self.manager_name
        )

    @cached_property
    def all_net_cfs(self) -> pd.DataFrame:
        entity_info = self.manager_hierarchy_structure.get_entities_directly_related_by_name(
            EntityDomainTypes.Investment
        )
        sources = entity_info[
            [
                EntityStandardNames.SourceName,
                EntityStandardNames.ExternalId,
            ]
        ]
        f_sources = sources[
            sources[EntityStandardNames.SourceName] == "IDW.PVM.TR"
        ]
        investment_ids = [
            int(x)
            for x in f_sources[EntityStandardNames.ExternalId]
            .drop_duplicates()
            .to_list()
        ]
        df = get_cfs(investment_ids, "Investment")
        return df

    @cached_property
    def all_position_cfs(self) -> pd.DataFrame:

        inv_entity_info = self.manager_hierarchy_structure.get_entities_directly_related_by_name(
            EntityDomainTypes.Investment
        )
        position_list: List[int] = (
            get_positions(
                self.manager_hierarchy_structure, inv_entity_info
            )["PositionId"]
            .drop_duplicates()
            .to_list()
        )
        df = get_cfs(position_list, "Position")
        # merge against position and Asset Id
        final_df = get_position_map(df, position_list)
        return final_df


class TrackRecordManagerSingletonProvider(metaclass=Singleton):
    def __init__(self):
        self._cache = {}

    def get_manager_tr_info(self, manager_name) -> TrackRecordHandler:
        if manager_name not in self._cache:
            m = TrackRecordHandler(manager_name)
            self._cache[manager_name] = m
        return self._cache[manager_name]
