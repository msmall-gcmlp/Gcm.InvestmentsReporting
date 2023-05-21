from gcm.inv.utils.misc.table_cache_base import Singleton

from ......core.entity_handler import (
    EntityDomainTypes,
    EntityStandardNames,
    List,
)
from gcm.inv.dataprovider.entity_provider.hierarchy_controller.hierarchy_handler import (
    HierarchyHandler,
)
from functools import cached_property
import pandas as pd
from .gets.get_cfs import get_cfs
from .gets.get_positions import get_positions, get_assets
from .gets.get_dimns import get_dimns


class TrackRecordHandler(object):
    def __init__(self, manager_name: str):
        self.manager_name = manager_name

    @cached_property
    def manager_hierarchy_structure(self) -> HierarchyHandler:
        return HierarchyHandler(
            EntityDomainTypes.InvestmentManager, self.manager_name
        )

    @cached_property
    def investments(self):
        h = self.manager_hierarchy_structure
        inv_entity_info = h.get_entities_directly_related_by_name(
            EntityDomainTypes.Investment
        )
        return inv_entity_info

    @staticmethod
    def get_idw_pvm_tr_ids(entity_info: pd.DataFrame):
        sources = entity_info[
            [
                EntityStandardNames.SourceName,
                EntityStandardNames.ExternalId,
            ]
        ]

        # DT: do we want this filtering done beforehand
        f_sources = sources[
            sources[EntityStandardNames.SourceName] == "IDW.PVM.TR"
        ]
        ids = [
            int(x)
            for x in f_sources[EntityStandardNames.ExternalId]
            .drop_duplicates()
            .to_list()
        ]
        return ids

    @property
    def investment_ids(self):
        entity_info = self.investments
        return TrackRecordHandler.get_idw_pvm_tr_ids(entity_info)

    @cached_property
    def position_ids(self):
        position_list: List[int] = (
            get_positions(self.raw_asset_map)["PositionId"]
            .drop_duplicates()
            .to_list()
        )
        return position_list

    @cached_property
    def raw_asset_map(self) -> pd.DataFrame:
        entity_info = self.investments
        return get_assets(self.manager_hierarchy_structure, entity_info)

    @cached_property
    def asset_ids(self):
        entity_info = self.raw_asset_map
        return TrackRecordHandler.get_idw_pvm_tr_ids(entity_info)

    @cached_property
    def all_inv_cfs(self) -> pd.DataFrame:
        # DT: creating entity_info down to sources here again?
        df = get_cfs(self.investment_ids, "Investment")
        return df

    @cached_property
    def position_cf(self) -> pd.DataFrame:
        df = get_cfs(self.position_ids, "Position")
        return df

    @cached_property
    def manager_attrib(self) -> pd.DataFrame:
        raise RuntimeError()

    @cached_property
    def investment_attrib(self) -> pd.DataFrame:
        investment_ids = self.investment_ids
        inv = get_dimns(investment_ids, "Investment")
        return inv

    @cached_property
    def position_attrib(self) -> pd.DataFrame:
        position_ids = self.position_ids
        pos = get_dimns(position_ids, "Position")
        assets = self.asset_attribs
        pos = pd.merge(pos, assets, on="AssetId")
        return pos

    @cached_property
    def asset_attribs(self) -> pd.DataFrame:
        asset_ids = self.asset_ids
        asset = get_dimns(asset_ids, "Asset")
        return asset


class TrackRecordManagerSingletonProvider(metaclass=Singleton):
    def __init__(self):
        self._cache = {}

    def get_manager_tr_info(self, manager_name) -> TrackRecordHandler:
        if manager_name not in self._cache:
            m = TrackRecordHandler(manager_name)
            self._cache[manager_name] = m
        return self._cache[manager_name]
