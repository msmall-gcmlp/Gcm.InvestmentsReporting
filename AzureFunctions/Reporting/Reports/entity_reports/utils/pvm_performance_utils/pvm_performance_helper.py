from gcm.inv.entityhierarchy.EntityDomain.entity_domain import (
    EntityDomainTypes,
    pd,
    Standards as EntityStandardNames,
)
from .helpers import (
    convert_amt_to_usd,
    get_ilevel_cfs,
    get_to_usd_fx_rates,
)
from .helpers.singleton_helpers import (
    get_all_os_for_all_portfolios,
    get_all_deal_attributes,
)
from gcm.inv.utils.misc.table_cache_base import Singleton
from typing import List
from enum import Enum, auto
import datetime as dt
from .analytics import get_twror_by_industry_rpt


class PvmPerfomanceHelperSingleton(metaclass=Singleton):
    _OS_Series_Identifier = "OperationalSeriesTicker"

    def __init__(self):
        pass

    @property
    def all_operational_series(self):
        _name = "__all_os_for_known_portfolios"
        if getattr(self, _name, None) is None:
            df = get_all_os_for_all_portfolios()
            setattr(self, _name, df)
        return getattr(self, _name, df)

    @property
    def usd_conversion_table(self) -> pd.DataFrame:
        _name = "__usd_convers"
        if getattr(self, _name, None) is None:
            df = get_to_usd_fx_rates()
            setattr(self, _name, df)
        return getattr(self, _name, None)

    @property
    def all_deal_attributes(self) -> pd.DataFrame:
        __name = "__all_deal_attributes"
        if getattr(self, __name, None) is None:
            df = get_all_deal_attributes()
            setattr(self, __name, df)
        return getattr(self, __name, None)


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
        # raw_cfs = self.associated_raw_ilevel_cfs_and_deal_data
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.AllCashflows:
            raw_df = self.converted_usd_ilevel_cfs.copy()
            raw_df = raw_df[raw_df.TransactionDate <= as_of_date]
            return raw_df
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.IrrCashflows:
            raw_df = self.converted_usd_ilevel_cfs.copy()
            max_nav_date = (
                raw_df[raw_df.TransactionType == "Net Asset Value"]
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
            raw_df = raw_df[raw_df.TransactionDate <= as_of_date]
            irr_cf = raw_df[raw_df.TransactionDate <= raw_df.MaxNavDate]
            irr_cf = irr_cf[irr_cf.TransactionType.isin(['Contributions - Investments and Expenses',
                                                        'Distributions - Recallable',
                                                        'Distributions - Return of Cost',
                                                        'Distributions - Gain/(Loss)',
                                                        'Distributions - Outside Interest',
                                                        'Distributions - Dividends and Interest',
                                                        'Contributions - Outside Expenses',
                                                        'Contributions - Contra Contributions',
                                                        'Contributions - Inside Expenses (DNAU)',
                                                        'Distributions - Escrow Receivables'])]
            latest_reported_nav = raw_df[(raw_df.TransactionDate == raw_df.MaxNavDate) & (raw_df.TransactionType == 'Net Asset Value')]
            rslt = pd.concat([irr_cf, latest_reported_nav]).sort_values('TransactionDate').reset_index(drop=True)
            return rslt
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.NavTimeSeries:
            raw_df = self.converted_usd_ilevel_cfs.copy()
            raw_df = raw_df[raw_df.TransactionDate <= as_of_date]
            rslt = raw_df[raw_df.TransactionType == 'Net Asset Value'].sort_values('TransactionDate').reset_index(drop=True)
            return rslt
        if cf_type == PvmPerformanceHelper.Cf_Filter_Type.CommitmentSeries:
            raw_df = self.converted_usd_ilevel_cfs.copy()
            raw_df = raw_df[raw_df.TransactionDate <= as_of_date]
            commitment_df = raw_df[raw_df.TransactionType.isin(['Contributions - Investments and Expenses',
                'Distributions - Recallable',
                'Contributions - Contra Contributions',
                'Contributions - Outside Expenses (AU)',
                'Unfunded Commitment Without Modification',
                'Local Discounted Commitments (For USD Holdings in Foreign Portfolios'])].rename(columns={'BaseAmount': 'Commitment'})
            unfunded = commitment_df[
                (
                        commitment_df.TransactionType
                        == "Unfunded Commitment Without Modification"
                )
                & (commitment_df.TransactionDate == as_of_date)
                ]
            funded = commitment_df[
                commitment_df.TransactionType
                != "Unfunded Commitment Without Modification"
                ]
            funded = (
                funded[["OwnerName", "InvestmentName", "Commitment"]]
                    .groupby(["OwnerName", "InvestmentName"])
                    .sum()
                    .reset_index()
            )

            rslt = pd.concat(
                [unfunded[["OwnerName", "InvestmentName", "Commitment"]], funded]
            )
            rslt = rslt.groupby(["OwnerName", "InvestmentName"]).sum().reset_index()
            return rslt

        raise NotImplementedError()

    @property
    def related_operational_series(self) -> pd.DataFrame:
        __name = "__related_os"
        _item = getattr(self, __name, None)
        if _item is None:
            # do work to get series
            df: pd.DataFrame = None
            all_os = PvmPerfomanceHelperSingleton().all_operational_series
            if self.entity_domain == EntityDomainTypes.Portfolio:
                df = all_os[
                    all_os[f"{self.entity_domain.name}ReportingName"].isin(
                        self.entity_info[
                            EntityStandardNames.EntityName
                        ].to_list()
                    )
                ]
            elif self.entity_domain == EntityDomainTypes.InvestmentManager:
                raise NotImplementedError()
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
                ]
            elif self.entity_domain == EntityDomainTypes.Portfolio:
                list_of_items = [
                    ["Portfolio"],
                    ["PredominantInvestmentType"],
                    ["PredominantInvestmentType", "PredominantSector"],
                    ["Name"],
                ]
            setattr(self, __name, list_of_items)
        return getattr(self, __name, None)

    @property
    def trailing_periods(self, as_of_date) -> dict:
        return {'QTD': 1,
                'YTD': int(as_of_date.month / 3),
                'TTM': 4,
                '3Y': 12,
                '5Y': 20,
                'ITD': 'ITD'}
    @property
    def attributes_needed(self) -> List[str]:
        if self.entity_domain in [
            EntityDomainTypes.InvestmentManager,
            EntityDomainTypes.Portfolio,
        ]:
            return [
                "Name",
                "PredominantInvestmentType",
                "PredominantSector",
                "PredominantRealizationTypeCategory",
            ]
        else:
            print('idk u didnt do it right')

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
                raise NotImplementedError()
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
        full_cfs = pd.concat([irr_cfs, nav_df])
        commitment_df = self.get_cfs_of_type(
            as_of_date,
            PvmPerformanceHelper.Cf_Filter_Type.CommitmentSeries,
            reporting_type,
        )
        tmp_trailing_period = {'QTD': 1,
                'YTD': int(as_of_date.month / 3),
                'TTM': 4,
                '3Y': 12,
                '5Y': 20,
                'ITD': 'ITD'}
        data = get_twror_by_industry_rpt(
            owner=self.top_line_owner,
            list_to_iterate=self.recursion_iterate_controller,
            irr_cfs=irr_cfs,
            full_cfs=full_cfs,
            nav_df=nav_df,
            commitment_df=commitment_df,
            as_of_date=as_of_date,
            _attributes_needed=self.attributes_needed,
            _trailing_periods=tmp_trailing_period
        )
        return data
