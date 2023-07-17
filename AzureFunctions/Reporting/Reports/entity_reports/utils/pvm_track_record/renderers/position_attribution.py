from ..base_pvm_tr_report import BasePvmTrackRecordReport

from .position_summary import (
    PositionSummarySheet,
    ReportingLayerAggregatedResults,
    ReportTable,
    ReportWorksheet,
)
from typing import List
import pandas as pd


class PositionAttribution(PositionSummarySheet):
    def __init__(
        self, report: BasePvmTrackRecordReport, attribution_item: List[str]
    ):
        super().__init__(report)
        self.attribution_item = attribution_item

    def render_df(
        self, item: ReportingLayerAggregatedResults
    ) -> pd.DataFrame:
        df = item.to_df()
        num_inv = "# investments"
        df[num_inv] = item.full_expanded_performance_results_count
        measure_types = item.__class__.Measures
        known_measures = [
            measure_types.cost,
            measure_types.distributions,
            measure_types.nav,
            measure_types.moic,
            measure_types.irr,
        ]
        final_measures = ["Name"] + [x.name for x in known_measures]
        final_measures.append(num_inv)
        df = df[final_measures]
        return df

    def to_worksheet(self) -> ReportWorksheet:
        # run attribution
        attribution_results = (
            self.report.pvm_perfomance_results.position_attribution(
                self.attribution_item
            ).results()
        )
        df_cache = []
        for i in attribution_results.sub_layers:
            item: ReportingLayerAggregatedResults = i
            df = self.render_df(item)
            df_cache.append(df)
        final = pd.concat(df_cache)
        final.reset_index(inplace=True, drop=True)
        table = ReportTable("AttributionResults", final)
        return ReportWorksheet(
            "Fund TR",
            ReportWorksheet.ReportWorkSheetRenderer(),
            report_tables=[table],
        )
