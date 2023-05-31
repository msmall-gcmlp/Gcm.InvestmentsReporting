from gcm.inv.entityhierarchy.EntityDomain.entity_domain import (
    EntityDomainTypes,
    pd,
    Standards as EntityStandardNames,
)
from .analytics import get_performance_report_dict
from .helpers import (
    convert_amt_to_usd,
    get_ilevel_cfs,
    get_to_usd_fx_rates,
)
from .helpers.singleton_helpers import (
    get_all_os_for_all_portfolios,
    get_all_deal_attributes,
    get_all_manager_holdings,
    get_burgiss_bmark,
)
from gcm.inv.utils.misc.table_cache_base import Singleton
from typing import List
from enum import Enum, auto
import datetime as dt
import numpy as np
from functools import cached_property


class PvmPerfomanceHelperSingleton(metaclass=Singleton):
    _OS_Series_Identifier = "OperationalSeriesTicker"

    def __init__(self):
        pass

    @cached_property
    def all_operational_series(self):
        return get_all_os_for_all_portfolios()

    @cached_property
    def all_manager_holdings(self):
        return get_all_manager_holdings()

    @cached_property
    def usd_conversion_table(self) -> pd.DataFrame:
        return get_to_usd_fx_rates()

    @cached_property
    def all_deal_attributes(self) -> pd.DataFrame:
        return get_all_deal_attributes()

    @cached_property
    def benchmark_df(self) -> pd.DataFrame:
        return get_burgiss_bmark()


class PvmPerformanceHelper(object):
    def __init__(
        self, entity_domain: EntityDomainTypes, entity_info: pd.DataFrame
    ):
        self.entity_domain = entity_domain
        self.entity_info = entity_info

    class Cf_Filter_Type(Enum):
        AllCashflows = auto()
        IrrCashflows = auto()
        NavTimeSeries = auto()
        CommitmentSeries = auto()

    class ReportedCfType(Enum):
        AMV = auto()
        RMV = auto()

    def get_cfs_of_type(
        self,
        as_of_date: dt.date,
        cf_type: "Cf_Filter_Type" = Cf_Filter_Type.AllCashflows,
        reporting_type: "ReportedCfType" = ReportedCfType.RMV,
    ) -> pd.DataFrame:
        # TODO: below is auto converted to USD. Make it more dynamic
        raw_df = self.converted_usd_ilevel_cfs
        raw_df = raw_df[raw_df.TransactionDate <= as_of_date]
        if self.entity_domain == EntityDomainTypes.InvestmentManager:
            raw_df = raw_df[
                raw_df.InvestmentName.isin(
                    self.related_mgr_holdings.HoldingName
                )
            ]
        if self.entity_domain == EntityDomainTypes.Vertical:
            # ad hoc/temporary
            raw_df = raw_df[
                raw_df.PredominantInvestmentType.isin(
                    [
                        "Co-investment/Direct",
                        # 'Primary Fund',
                        # 'Secondary'
                    ]
                )
            ]
            raw_df = raw_df[
                raw_df.PredominantAssetClass.isin(["Private Equity"])
            ]
            # raw_df = raw_df[
            #     raw_df.PredominantAssetClass.isin(["Infrastructure"])
            # ]
            # raw_df = raw_df[raw_df.VintageYear >= 2009]
            raw_df = raw_df[~((raw_df.PredominantInvestmentType == 'Secondary') & (raw_df.VintageYear < 2014))]
            raw_df = raw_df[~((raw_df.PredominantAssetClass == 'Real Estate') & (raw_df.VintageYear < 2010))]

        # TODO: DT note figure out whether we want max nav date to reflect NAV != 0
        # makes a difference for all RMV measures at group levels (vertical/portfolio/sector/etc)
        max_nav_date = (
            raw_df[
                (raw_df.TransactionType == "Net Asset Value")
                # & (raw_df.BaseAmount != 0)
            ]
            .groupby(["OwnerName", "InvestmentName"])
            .TransactionDate.max()
            .reset_index()
            .rename(columns={"TransactionDate": "MaxNavDate"})
        )
        raw_df = raw_df.merge(
            max_nav_date,
            how="left",
            left_on=["OwnerName", "InvestmentName"],
            right_on=["OwnerName", "InvestmentName"],
        )
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.AllCashflows:
            return raw_df
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.IrrCashflows:
            irr_cf = raw_df[raw_df.TransactionDate <= raw_df.MaxNavDate]
            #TODO: move to ENUMs and only use R/D/T for IRR cfs
            irr_cf = irr_cf[
                irr_cf.TransactionType.isin(
                    [
                        "Contributions - Investments and Expenses",
                        "Distributions - Recallable",
                        "Distributions - Return of Cost",
                        "Distributions - Gain/(Loss)",
                        "Distributions - Outside Interest",
                        "Distributions - Dividends and Interest",
                        "Contributions - Outside Expenses",
                        "Contributions - Contra Contributions",
                        "Contributions - Inside Expenses (DNAU)",
                        "Distributions - Escrow Receivables",
                    ]
                )
            ]
            irr_cf["TransactionType"] = np.where(
                irr_cf.TransactionType.str.contains("Contributions -"),
                "Contributions",
                irr_cf.TransactionType,
            )
            irr_cf["TransactionType"] = np.where(
                irr_cf.TransactionType.str.contains("Distributions -"),
                "Distributions",
                irr_cf.TransactionType,
            )
            irr_cf.BaseAmount = irr_cf.BaseAmount * -1
            latest_reported_nav = raw_df[
                (raw_df.TransactionDate == raw_df.MaxNavDate)
                & (raw_df.TransactionType == "Net Asset Value")
            ]
            irr_cf_rslt = (
                pd.concat([irr_cf, latest_reported_nav])
                .sort_values("TransactionDate")
                .reset_index(drop=True)
            )
            return irr_cf_rslt
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.NavTimeSeries:
            nav_df = (
                raw_df[raw_df.TransactionType == "Net Asset Value"]
                .sort_values("TransactionDate")
                .reset_index(drop=True)
            )
            return nav_df
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.CommitmentSeries:
            commitment_df = raw_df[
                raw_df.TransactionType.isin(
                    [
                        "Contributions - Investments and Expenses",
                        "Distributions - Recallable",
                        "Contributions - Contra Contributions",
                        "Contributions - Outside Expenses (AU)",
                        "Unfunded Commitment Without Modification",
                        "Local Discounted Commitments (For USD Holdings in Foreign Portfolios",
                    ]
                )
            ].rename(columns={"BaseAmount": "Commitment"})
            unfunded = commitment_df[
                (
                    commitment_df.TransactionType
                    == "Unfunded Commitment Without Modification"
                )
                & (commitment_df.TransactionDate == as_of_date)
            ]
            funded = commitment_df[
                (
                    commitment_df.TransactionType
                    != "Unfunded Commitment Without Modification"
                )
                & (
                    commitment_df.TransactionDate
                    <= commitment_df.MaxNavDate
                )
            ]
            funded = (
                funded[["OwnerName", "InvestmentName", "Commitment"]]
                .groupby(["OwnerName", "InvestmentName"])
                .sum()
                .reset_index()
            )

            commitment_df = pd.concat(
                [
                    unfunded[
                        ["OwnerName", "InvestmentName", "Commitment"]
                    ],
                    funded,
                ]
            )
            commitment_df = (
                commitment_df.groupby(["OwnerName", "InvestmentName"])
                .sum()
                .reset_index()
            )
            commitment_df_rslt = commitment_df.merge(
                self.this_entities_related_deal_info,
                how="left",
                left_on=["OwnerName", "InvestmentName"],
                right_on=["OsTicker", "ReportingName"],
            )
            assert len(commitment_df) == len(commitment_df_rslt)

            return commitment_df_rslt

        raise NotImplementedError()

    @property
    def related_operational_series(self) -> pd.DataFrame:
        __name = "__related_os"
        _item = getattr(self, __name, None)
        if _item is None:
            df: pd.DataFrame = None
            all_os = PvmPerfomanceHelperSingleton().all_operational_series
            all_mgrs = PvmPerfomanceHelperSingleton().all_manager_holdings
            if self.entity_domain == EntityDomainTypes.Portfolio:
                df = all_os[
                    all_os[f"{self.entity_domain.name}ReportingName"].isin(
                        self.entity_info[
                            EntityStandardNames.EntityName
                        ].to_list()
                    )
                ]
            elif self.entity_domain == EntityDomainTypes.InvestmentManager:
                df = all_mgrs[
                    all_mgrs[f"{self.entity_domain.name}Name"].isin(
                        self.entity_info[
                            EntityStandardNames.EntityName
                        ].to_list()
                    )
                ]
            elif self.entity_domain == EntityDomainTypes.Vertical:
                df = all_os
            setattr(self, __name, df)

        return getattr(self, __name, None)

    @property
    def related_mgr_holdings(self) -> pd.DataFrame:
        __name = "__related_mgr_holdings"
        _item = getattr(self, __name, None)
        if _item is None:
            # do work to get series
            df: pd.DataFrame = None
            all_mgrs = PvmPerfomanceHelperSingleton().all_manager_holdings
            if self.entity_domain == EntityDomainTypes.InvestmentManager:
                df = all_mgrs[
                    all_mgrs.InvestmentManagerName.isin(
                        self.entity_info[
                            EntityStandardNames.EntityName
                        ].to_list()
                    )
                ]
            setattr(self, __name, df)
        return getattr(self, __name, None)

    @property
    def os_tickers(self) -> List[str]:
        os_list = (
            self.related_operational_series[
                PvmPerfomanceHelperSingleton._OS_Series_Identifier
            ]
            .drop_duplicates()
            .to_list()
        )
        return os_list

    @property
    def associated_raw_ilevel_cfs_and_deal_data(self) -> pd.DataFrame:
        __name = "__os_cfs"
        _item = getattr(self, __name, None)
        if _item is None:
            df: pd.DataFrame = get_ilevel_cfs(self.os_tickers)
            rslt = df.merge(
                self.this_entities_related_deal_info,
                how="left",
                left_on=["OwnerName", "InvestmentName"],
                right_on=["OsTicker", "ReportingName"],
            )
            assert len(rslt) == len(df)
            setattr(self, __name, rslt)
        return getattr(self, __name, None)

    @property
    def converted_usd_ilevel_cfs(self) -> pd.DataFrame:
        __name = "__converted_cfs"
        _item = getattr(self, __name, None)
        if _item is None:
            cfs = self.associated_raw_ilevel_cfs_and_deal_data
            converted_cfs = convert_amt_to_usd(
                cfs, PvmPerfomanceHelperSingleton().usd_conversion_table
            )
            setattr(self, __name, converted_cfs)
        return getattr(self, __name, None)

    @property
    def this_entities_related_deal_info(self) -> pd.DataFrame:
        __name = "__my_deal_info"
        if getattr(self, __name, None) is None:
            df = PvmPerfomanceHelperSingleton().all_deal_attributes
            filtered = df[df["OsTicker"].isin(self.os_tickers)]
            filtered["Portfolio"] = self.top_line_owner
            filtered['Name'] = filtered.apply(
                lambda x: "_".join([str(x[i]) for i in self.recursion_iterate_controller[-1]]), axis=1
            )
            setattr(self, __name, filtered)
        return getattr(self, __name, None)

    @property
    def recursion_iterate_controller(self) -> List[List[str]]:
        __name = "recursion_iterate"
        if getattr(self, __name, None) is None:
            list_of_items = None
            if self.entity_domain == EntityDomainTypes.InvestmentManager:
                list_of_items = [
                    ["Portfolio"],
                    ["PredominantInvestmentType"],
                    ["PredominantInvestmentType", "PredominantSector"],
                    [
                        "PredominantInvestmentType",
                        "PredominantSector",
                        "PredominantRealizationTypeCategory",
                    ],
                    ["Name"],
                ]
            elif self.entity_domain == EntityDomainTypes.Portfolio:
                list_of_items = [
                    ["Portfolio"],
                    # ["PredominantInvestmentType"],
                    # ["PredominantInvestmentType", "PredominantSector"],
                    ["Name"],
                ]
                # list_of_items = [
                #     ["Portfolio"],
                #     ["PredominantRealizationTypeCategory"],
                #     ["PredominantRealizationTypeCategory", "VintageYear"],
                #     ["Name"],
                # ]
            elif self.entity_domain == EntityDomainTypes.Vertical:
                list_of_items = [
                    ["Portfolio"],
                    ["Portfolio", "PredominantInvestmentType"],
                    ["Portfolio", "PredominantInvestmentType", "EmergingDiverseStatus"],
                    ["Portfolio", "PredominantInvestmentType", "EmergingDiverseStatus", "DealName"],
                ]
                # list_of_items = [
                #     ["Portfolio"],
                #     ["PredominantInvestmentType"],
                #     ["Name"],
                # ]
            setattr(self, __name, list_of_items)
        return getattr(self, __name, None)

    @property
    def trailing_periods(self) -> dict:
        # TODO: make this work so not using tmp_trailing_periods
        # TODO: also probably should use standard trailing_period enums
        as_of_date = Scenario.get
        return {
            "QTD": 1,
            "YTD": int(as_of_date.month / 3),
            "TTM": 4,
            "3Y": 12,
            "5Y": 20,
            "ITD": "ITD",
        }

    @property
    def attributes_needed(self) -> List[str]:
        if self.entity_domain == EntityDomainTypes.Portfolio:
            return [
                "Name",
                # "PredominantInvestmentType",
                # "PredominantSector",
            ]
            # return [
            #     "Name",
            #     "PredominantRealizationTypeCategory",
            #     "VintageYear",
            # ]
        elif self.entity_domain == EntityDomainTypes.Vertical:
            # return [
            #     "Name",
            #     "PredominantInvestmentType",
            #     "PredominantSector",
            # ]
            # TODO: refactor "Name" to be Atomic unit or equiv; Node ID/Edge ID?
            return [
                "Name",
                "DealName",
                "PredominantInvestmentType",
                "EmergingDiverseStatus"
            ]
        elif self.entity_domain == EntityDomainTypes.InvestmentManager:
            return [
                "Name",
                "PredominantInvestmentType",
                "PredominantSector",
                "PredominantRealizationTypeCategory",
            ]

    @property
    def top_line_owner(self) -> str:
        __name = "__top_line"
        if getattr(self, __name, None) is None:
            if self.entity_domain == EntityDomainTypes.Portfolio:
                tickers = (
                    self.related_operational_series["PortfolioTicker"]
                    .drop_duplicates()
                    .to_list()
                )
                assert len(tickers) == 1
                setattr(self, __name, tickers[0])
            elif self.entity_domain == EntityDomainTypes.InvestmentManager:
                tickers = (
                    self.related_mgr_holdings["InvestmentManagerName"]
                    .drop_duplicates()
                    .to_list()
                )
                assert len(tickers) == 1
                setattr(self, __name, tickers[0])
            elif self.entity_domain == EntityDomainTypes.Vertical:
                # ad hoc/temporary
                tickers = ["GCM Private Equity"]
                assert len(tickers) == 1
                setattr(self, __name, tickers[0])
        return getattr(self, __name, None)

    def generate_components_for_this_entity(
        self, as_of_date: dt.date
    ) -> dict[str, pd.DataFrame]:
        reporting_type = PvmPerformanceHelper.ReportedCfType.RMV
        irr_cfs = self.get_cfs_of_type(
            as_of_date,
            PvmPerformanceHelper.Cf_Filter_Type.IrrCashflows,
            reporting_type,
        )
        nav_df = self.get_cfs_of_type(
            as_of_date,
            PvmPerformanceHelper.Cf_Filter_Type.NavTimeSeries,
            reporting_type,
        )
        full_cfs = pd.concat(
            [irr_cfs[irr_cfs.TransactionType != "Net Asset Value"], nav_df]
        )
        commitment_df = self.get_cfs_of_type(
            as_of_date,
            PvmPerformanceHelper.Cf_Filter_Type.CommitmentSeries,
            reporting_type,
        )
        # TODO: move to argument/scenario/standard config
        tmp_trailing_period = {
            "QTD": 1,
            "YTD": int(as_of_date.month / 3),
            "TTM": 4,
            "3Y": 12,
            "5Y": 20,
            "ITD": "ITD",
        }
        report_json = get_performance_report_dict(
            owner=self.top_line_owner,
            list_to_iterate=self.recursion_iterate_controller,
            irr_cfs=irr_cfs,
            full_cfs=full_cfs,
            nav_df=nav_df,
            commitment_df=commitment_df,
            as_of_date=as_of_date,
            _attributes_needed=self.attributes_needed,
            _trailing_periods=tmp_trailing_period,
        )

        # more to do on the below
        report_json.update(
            {
                "Date": pd.DataFrame({"Date": [as_of_date]}),
                "PortfolioName": pd.DataFrame(
                    {"PortfolioName": [self.top_line_owner]},
                ),
                "benchmark_df": PvmPerfomanceHelperSingleton().benchmark_df,
            }
        )

        return report_json
