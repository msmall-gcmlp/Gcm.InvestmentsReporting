from ...pvm_track_record.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
    PvmPerformanceResultsBase,
)
from .position_summary import (
    PositionSummarySheet,
    ReportWorksheet,
    ReportTable,
)
import pandas as pd
from functools import cached_property


class PositionConcentration(PositionSummarySheet):
    def __init__(self, report: BasePvmTrackRecordReport):
        super().__init__(report)

    def append_percent_realized_performance(self):
        pass

    @cached_property
    def total_1_3_5_other(self) -> dict[object, PvmPerformanceResultsBase]:
        return self.report._1_3_5_objects(
            self.report.total_positions_line_item
        )

    @cached_property
    def realized_1_3_5_other(
        self,
    ) -> dict[object, PvmPerformanceResultsBase]:
        item = self.report._1_3_5_objects(
            self.report.realized_reporting_layer
        )
        return item

    top_deal_count = 5

    _percent_realized_gain = "percent realized gain"
    _percent_capital = "percent_in_group_capital"

    def append_relevant_info(self, df: pd.DataFrame):
        df = super().append_relevant_info(df)
        cls = self.__class__
        df[cls._percent_realized_gain] = (
            df[cls.base_measures.pnl.name] / self.total_realized.pnl
        )
        return df

    def all_gross_investments_formatted(self) -> pd.DataFrame:
        base = super().all_gross_investments_formatted()
        return self.construct_rendered_frame(
            base, PositionConcentration.standard_cols
        )

    def total_realized_investments_formatted(self) -> pd.DataFrame:
        base = super().total_realized_investments_formatted()
        return self.construct_rendered_frame(
            base, PositionConcentration.standard_cols
        )

    standard_cols = [
        PositionSummarySheet.title,
        PositionSummarySheet.report_measures.investment_date.name,
        PositionSummarySheet.report_measures.exit_date.name,
        PositionSummarySheet.base_measures.cost.name,
        PositionSummarySheet.base_measures.unrealized_value.name,
        PositionSummarySheet.base_measures.total_value.name,
        PositionSummarySheet.base_measures.pnl.name,
        PositionSummarySheet.base_measures.moic.name,
        PositionSummarySheet.base_measures.irr.name,
        _percent_realized_gain,
        PositionSummarySheet._percent_total_gain,
    ]

    concentration_columns = [
        PositionSummarySheet.base_measures.irr.name,
        PositionSummarySheet.base_measures.moic.name,
        _percent_realized_gain,
        PositionSummarySheet._percent_total_gain,
    ]

    distribution_columns = [
        PositionSummarySheet.base_measures.full_expanded_performance_results_count.name,
        PositionSummarySheet.base_measures.moic.name,
        PositionSummarySheet.base_measures.irr.name,
        _percent_capital,
    ]

    def top_in_group_deals_df(
        self,
        deal_set: pd.DataFrame,
        set_1_3_5: dict[object, PvmPerformanceResultsBase],
    ):
        cls = self.__class__
        filtered_deal_set = deal_set.head(cls.top_deal_count)
        other_key = int(-1 * cls.top_deal_count)
        if other_key in set_1_3_5:
            other = set_1_3_5[other_key]
            other = self.create_item_df(
                other, PositionConcentration.standard_cols
            )
            filtered_deal_set = pd.concat([filtered_deal_set, other])
        return self.construct_rendered_frame(
            filtered_deal_set, PositionConcentration.standard_cols
        )

    def top_all_deals_df(self):
        sort_by = PositionSummarySheet.base_measures.pnl.name
        realized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED,
            sort_by,
        )
        unrealized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.UNREALIZED,
            sort_by,
        )
        all_deals: pd.DataFrame = pd.concat([realized_df, unrealized_df])
        return self.top_in_group_deals_df(
            all_deals, self.total_1_3_5_other
        )

    def top_realized_deals_df(self):
        sort_by = PositionSummarySheet.base_measures.pnl.name
        realized_df = self.position_breakout(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED,
            sort_by,
        )
        return self.top_in_group_deals_df(
            realized_df, self.realized_1_3_5_other
        )

    def to_worksheet(self) -> ReportWorksheet:
        d_items = {
            "top_deals": self.top_all_deals_df,
            "top_deals_total": self.all_gross_investments_formatted,
            "top_realized_deals": self.top_realized_deals_df,
            "top_realized_deals_total": self.total_realized_investments_formatted,
            # "all_concen": None,
            # "all_concen_total": None,
            # "realized_concen": None,
            # "realized_concen_total": None,
            # "all_distrib": None,
            # "all_distrib_total": None,
            # "realized_distrib": None,
            # "realized_distrib_total": None,
        }
        to_render = []
        for k, v in d_items.items():
            df = v()
            i = ReportTable(k, df)
            to_render.append(i)
        return ReportWorksheet(
            "Performance Concentration",
            ReportWorksheet.ReportWorkSheetRenderer(),
            to_render,
        )
