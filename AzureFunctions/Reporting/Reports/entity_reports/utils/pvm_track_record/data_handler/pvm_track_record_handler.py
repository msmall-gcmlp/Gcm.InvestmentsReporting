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
from .gets.get_dimns import get_dimns, get_facts
from ...pvm_performance_results import (
    PvmPerformanceResultsBase,
    PvmCashflows,
    AggregateInterval,
)
from .gross_atom import GrossAttributionAtom
from enum import Enum, auto


class TrackRecordHandler(object):
    def __init__(self, manager_name: str):
        self.manager_name = manager_name
        self._position_level_cache = {}

    class CommonPositionAttribution(Enum):
        RealizationStatus = auto()
        Industry = auto()

    _position_attribution_map = {
        "Reported Realization Status": CommonPositionAttribution.RealizationStatus.name,
        "Realization Status": CommonPositionAttribution.RealizationStatus.name,
        "Industry category": CommonPositionAttribution.Industry.name,
    }

    @property
    def gross_atom(self):
        return GrossAttributionAtom.Position

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
        # remap to common items.
        # TODO: to be done in DB table
        remap_dict = {}
        execute_remap = False
        for c in pos.columns:
            if c in TrackRecordHandler._position_attribution_map:
                remap_dict[
                    c
                ] = TrackRecordHandler._position_attribution_map[c]
                execute_remap = True
        if execute_remap:
            pos.rename(columns=remap_dict, inplace=True)
        return pos

    @cached_property
    def position_facts(self) -> pd.DataFrame:
        position_ids = self.position_ids
        f = get_facts(position_ids, "Position")
        return f

    @cached_property
    def asset_attribs(self) -> pd.DataFrame:
        asset_ids = self.asset_ids
        asset = get_dimns(asset_ids, "Asset")
        return asset

    def gross_atom_level_performance_cache(
        self, aggregate_interval: AggregateInterval
    ) -> dict[int, PvmPerformanceResultsBase]:
        if aggregate_interval not in self._position_level_cache:
            grouped = self.position_cf.groupby(f"{self.gross_atom.name}Id")
            final: dict[int, PvmPerformanceResultsBase] = {}
            for n, g in grouped:
                cfs = g[[c.name for c in PvmCashflows.CashflowColumns]]
                cfs = PvmCashflows(cfs=cfs)
                final[n] = PvmPerformanceResultsBase(
                    cfs, aggregate_interval
                )
            real_final = {}
            position_facts = self.position_facts
            for k, v in final.items():
                filtered = position_facts[
                    position_facts["PositionId"] == k
                ]
                v = override(v, filtered, "cost")
                v = override(v, filtered, "distrib")
                v = override(v, filtered, "nav")
                real_final[k] = v
            self._position_level_cache[aggregate_interval] = real_final
        return self._position_level_cache[aggregate_interval]


def override(
    v: PvmPerformanceResultsBase,
    filtered_on_positions: pd.DataFrame,
    base_type: str,
) -> PvmPerformanceResultsBase:
    if base_type == "cost":
        total = sum(
            filtered_on_positions[
                filtered_on_positions["MeasureName"] == "Invested Capital"
            ]["MeasureValue"]
        )
        v.cost = total
    if base_type == "distrib":
        total = sum(
            filtered_on_positions[
                filtered_on_positions["MeasureName"] == "Realized Value"
            ]["MeasureValue"]
        )
        v.distributions = total
    if base_type == "nav":
        total = sum(
            filtered_on_positions[
                filtered_on_positions["MeasureName"] == "Unrealized Value"
            ]["MeasureValue"]
        )
        v.nav = total
    return v


class TrackRecordManagerSingletonProvider(metaclass=Singleton):
    def __init__(self):
        self._cache = {}

    def get_manager_tr_info(self, manager_name) -> TrackRecordHandler:
        if manager_name not in self._cache:
            m = TrackRecordHandler(manager_name)
            self._cache[manager_name] = m
        return self._cache[manager_name]
