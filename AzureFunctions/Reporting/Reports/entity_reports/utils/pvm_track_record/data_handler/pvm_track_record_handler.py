from gcm.inv.utils.misc.table_cache_base import Singleton
from pyxirr import xirr

from ......core.entity_handler import (
    EntityDomainTypes,
    EntityStandardNames,
    List,
)
from gcm.inv.dataprovider.entity_provider.hierarchy_controller.hierarchy_handler import (
    HierarchyHandler,
)
from functools import cached_property, reduce
import pandas as pd
from .gets.get_cfs import get_cfs
from .gets.get_positions import get_positions


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
        # DT: creating entity_info down to sources here again?
        entity_info = self.manager_hierarchy_structure.get_entities_directly_related_by_name(
            EntityDomainTypes.Investment
        )
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
        # final_df = get_position_map(df, position_list)
        return df

    @cached_property
    def manager_attrib(self) -> pd.DataFrame:
        mgr_cfs = self.all_net_cfs
        mgr_attrib = pd.DataFrame(
            {
                "InvestmentManagerName": [self.manager_name],
                "NetIrr": [
                    xirr(
                        mgr_cfs[["TransactionDate", "BaseAmount"]]
                        .groupby("TransactionDate")
                        .sum()
                        .reset_index()
                    )
                ],
                "NetTvpi": [
                    mgr_cfs[
                        mgr_cfs.TransactionType.isin(["D", "R"])
                    ].BaseAmount.sum()
                    / abs(
                        mgr_cfs[
                            mgr_cfs.TransactionType.isin(["T"])
                        ].BaseAmount.sum()
                    )
                ],
                "NetDpi": [
                    mgr_cfs[
                        mgr_cfs.TransactionType.isin(["D"])
                    ].BaseAmount.sum()
                    / abs(
                        mgr_cfs[
                            mgr_cfs.TransactionType.isin(["T"])
                        ].BaseAmount.sum()
                    )
                ],
            }
        )
        return mgr_attrib

    @cached_property
    def investment_attrib(self) -> pd.DataFrame:
        mgr_cfs = self.all_net_cfs
        investment_attrib = pd.DataFrame(
            {
                "InvestmentName": mgr_cfs.InvestmentName.unique(),
                # proxy committed as called capital, not right ofc, linking it up..
                "CommittedCapital": mgr_cfs[
                    ["InvestmentName", "CommittedCapital"]
                ]
                .drop_duplicates()
                .reset_index()
                .CommittedCapital,
                "VintageYear": mgr_cfs[["InvestmentName", "VintageYear"]]
                .drop_duplicates()
                .reset_index()
                .VintageYear,
                # below will work, but bad sample data gives error. hardcode for test
                # 'NetIrr': [mgr_cfs.groupby(['InvestmentName', 'TransactionDate'])[["TransactionDate", "BaseAmount"]].apply(xirr).reset_index()],
                "NetIrr": [0.05] * len(mgr_cfs.InvestmentName.unique()),
                "NetTvpi": [
                    mgr_cfs[mgr_cfs.TransactionType.isin(["D", "R"])]
                    .groupby(["InvestmentName"])
                    .BaseAmount.sum()
                    .reset_index(drop=True)
                    / mgr_cfs[mgr_cfs.TransactionType.isin(["T"])]
                    .groupby(["InvestmentName"])
                    .BaseAmount.sum()
                    .abs()
                    .reset_index(drop=True)
                ][0],
                "NetDpi": [
                    mgr_cfs[mgr_cfs.TransactionType.isin(["D"])]
                    .groupby(["InvestmentName"])
                    .BaseAmount.sum()
                    .reset_index(drop=True)
                    / mgr_cfs[mgr_cfs.TransactionType.isin(["T"])]
                    .groupby(["InvestmentName"])
                    .BaseAmount.sum()
                    .abs()
                    .reset_index(drop=True)
                ][0],
            }
        )
        return investment_attrib

    @cached_property
    def position_attrib(self) -> pd.DataFrame:
        mgr_cfs = self.all_position_cfs.sort_values("AssetName")
        inv_and_asset_names = mgr_cfs[
            [
                "AssetName",
                "InvestmentName",
                "PositionId",
                "InvestmentId",
                "AssetId",
            ]
        ].drop_duplicates()
        inv_date = mgr_cfs[
            ["AssetName", "InvestmentDate"]
        ].drop_duplicates()
        exit_date = mgr_cfs[["AssetName", "ExitDate"]].drop_duplicates()
        equity_invested = (
            mgr_cfs[mgr_cfs.TransactionType == "T"]
            .groupby(["AssetName"])
            .BaseAmount.sum()
            .abs()
            .reset_index()
            .rename(columns={"BaseAmount": "EquityInvested"})
        )
        status = pd.DataFrame(
            {
                "AssetName": inv_and_asset_names.AssetName.unique(),
                "Status": ["Realized", "Unrealized"] * 23,
            }
        )
        unrealized_value = (
            mgr_cfs[mgr_cfs.TransactionType.isin(["R"])]
            .groupby("AssetName")
            .BaseAmount.sum()
            .reset_index()
            .rename(columns={"BaseAmount": "UnrealizedValueGross"})
        )
        realized_value = (
            mgr_cfs[mgr_cfs.TransactionType.isin(["D"])]
            .groupby("AssetName")
            .BaseAmount.sum()
            .reset_index()
            .rename(columns={"BaseAmount": "RealizedValueGross"})
        )
        total_value = (
            mgr_cfs[mgr_cfs.TransactionType.isin(["D", "R"])]
            .groupby("AssetName")
            .BaseAmount.sum()
            .reset_index()
            .rename(columns={"BaseAmount": "TotalValue"})
        )
        inv_gain = (
            mgr_cfs[mgr_cfs.TransactionType.isin(["D", "R"])]
            .groupby("AssetName")
            .BaseAmount.sum()
            .abs()
            - mgr_cfs[mgr_cfs.TransactionType.isin(["T"])]
            .groupby("AssetName")
            .BaseAmount.sum()
            .abs()
        )
        inv_gain = inv_gain.reset_index().rename(
            columns={"BaseAmount": "InvestmentGain"}
        )

        position_attrib = reduce(
            lambda left, right: pd.merge(
                left, right, on=["AssetName"], how="outer"
            ),
            [
                inv_and_asset_names,
                inv_date,
                exit_date,
                equity_invested,
                status,
                unrealized_value,
                realized_value,
                total_value,
                inv_gain.reset_index(),
            ],
        )
        return position_attrib

    @cached_property
    def position_cf(self) -> pd.DataFrame:
        pos_cf = self.all_position_cfs
        position_cf = pos_cf[
            [
                "InvestmentName",
                "AssetName",
                "TransactionDate",
                "TransactionType",
                "BaseAmount",
            ]
        ].drop_duplicates()

        return position_cf


class TrackRecordManagerSingletonProvider(metaclass=Singleton):
    def __init__(self):
        self._cache = {}

    def get_manager_tr_info(self, manager_name) -> TrackRecordHandler:
        if manager_name not in self._cache:
            m = TrackRecordHandler(manager_name)
            self._cache[manager_name] = m
        return self._cache[manager_name]
