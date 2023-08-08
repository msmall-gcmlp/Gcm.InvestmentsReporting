from typing import NamedTuple
import pandas as pd
from gcm.inv.models.pvm.underwriting_analytics.perf_1_3_5 import (
    OneThreeFiveResult,
    RealizedUnrealized_135_Breakout,
)
from .top_deals import TopDeals_And_Total
from .grouped_135 import OneThreeFive_Bucketed_And_Total
from .return_distribution import ReturnDistributions
from ......core.components.report_worksheet import (
    ReportWorksheet,
    ReportTable,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao

TEMPLATE = AzureDataLakeDao.BlobFileStructure(
    zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
    sources="investmentsreporting",
    entity="exceltemplates",
    path=["PvmTrackRecordPerformanceConcentration.xlsx"],
)


class PerformanceConcentrationResultCluster(NamedTuple):
    Top_Deals_And_Total: TopDeals_And_Total
    Breakout_135: OneThreeFive_Bucketed_And_Total
    ReturnDistributions_Breakout: ReturnDistributions

    def to_dict(self) -> dict[str, pd.DataFrame]:
        return {
            "top_deals": self.Top_Deals_And_Total.Deal_Summary_Top_N_Deals_And_Other,
            "top_deals_total": self.Top_Deals_And_Total.Deal_Summary_Total,
            "concen": self.Breakout_135.Breakout_1_3_5_Bucketed_And_Other,
            "concen_total": self.Breakout_135.Breakout_1_3_5_Total,
            "distrib": self.ReturnDistributions_Breakout.Return_Distribution_Breakout,
            "distrib_total": self.ReturnDistributions_Breakout.Return_Distribution_Total,
        }


class PerformanceConcentration_Results(NamedTuple):
    All_Deals: PerformanceConcentrationResultCluster
    Realized_Deals: PerformanceConcentrationResultCluster
    Unrealized_Deals: PerformanceConcentrationResultCluster

    def to_dict(self):
        _items = [
            ("all", self.All_Deals),
            ("realized", self.Realized_Deals),
            ("unrealized", self.Unrealized_Deals),
        ]
        final = {}
        for i in _items:
            name = i[0]
            evaluated: PerformanceConcentrationResultCluster = i[1]
            this_dict = evaluated.to_dict()
            for k, v in this_dict.items():
                final[f"{name}_{k}"] = v
        return final


class OneThreeFiveRenderer(object):
    def __init__(
        self,
        breakout: RealizedUnrealized_135_Breakout,
        position_to_investment_mapping: pd.DataFrame,
        position_dimn: pd.DataFrame,
    ) -> None:
        self.breakout = breakout
        self.position_to_investment_mapping = (
            position_to_investment_mapping
        )
        self.position_dimn = position_dimn

    def _generate_clean_dfs(self):
        top_line = self.breakout.All.base_item

        def _apply_and_gen(
            _item: OneThreeFiveResult,
        ) -> PerformanceConcentrationResultCluster:

            inputs = {
                "this_result": _item,
                "top_line_node": top_line,
                "position_dimn": self.position_dimn,
                "investment_mapping": self.position_to_investment_mapping,
            }
            top_other_total = TopDeals_And_Total.generate(**inputs)
            one_three_five = OneThreeFive_Bucketed_And_Total.generate(
                **inputs
            )
            return_distribution = ReturnDistributions.generate(**inputs)
            return PerformanceConcentrationResultCluster(
                top_other_total, one_three_five, return_distribution
            )

        all = _apply_and_gen(self.breakout.All)
        realized = _apply_and_gen(self.breakout.Realized)
        unrealized = _apply_and_gen(self.breakout.Unrealized)
        all_expansion = PerformanceConcentration_Results(
            All_Deals=all,
            Realized_Deals=realized,
            Unrealized_Deals=unrealized,
        )
        return all_expansion

    def render(self) -> ReportWorksheet:
        cleaned: PerformanceConcentration_Results = (
            self._generate_clean_dfs()
        )
        d_items = cleaned.to_dict()
        to_render = []
        for k, df in d_items.items():
            i = ReportTable(k, df)
            to_render.append(i)
        return ReportWorksheet(
            "Performance Concentration",
            ReportWorksheet.ReportWorkSheetRenderer(),
            to_render,
        )
