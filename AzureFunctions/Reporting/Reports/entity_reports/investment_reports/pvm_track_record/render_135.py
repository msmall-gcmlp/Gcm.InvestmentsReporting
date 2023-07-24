from typing import NamedTuple
import pandas as pd
from gcm.inv.models.pvm.underwriting_analytics.perf_1_3_5 import (
    OneThreeFiveResult,
    RealizedUnrealized_135_Breakout,
    PvmNodeEvaluatable,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_item.to_df import (
    simple_display_df_from_evaluatable as df_evaluate,
    simple_display_df_from_list,
)
from enum import Enum, auto


class PerformanceConcentrationResultCluster(NamedTuple):
    Deal_Summary_Top_N_Deals_And_Other: pd.DataFrame
    Deal_Summary_Total: pd.DataFrame

    Breakout_1_3_5_Bucketed_And_Other: pd.DataFrame
    Breakout_1_3_5_Total: pd.DataFrame

    Return_Distribution_Breakout: pd.DataFrame
    Return_Distribution_Total: pd.DataFrame


class PerformanceConcentrationAll(NamedTuple):
    All_Deals: PerformanceConcentrationResultCluster
    Realized_Deals: PerformanceConcentrationResultCluster
    Unrealized_Deals: PerformanceConcentrationResultCluster


class PerformanceConcentrationRenderer:
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

    _percent = "PERCENT"

    class _OnTheFlyComputedColumnType(Enum):
        InGroup = auto()
        OnTopLine = auto()

    @staticmethod
    def compute_percent_of(
        inputted_frame: pd.DataFrame,
        top_line: PvmNodeEvaluatable,
        on_type: _OnTheFlyComputedColumnType,
        on_measure: PvmNodeEvaluatable.PvmEvaluationType,
    ):
        p = PerformanceConcentrationRenderer._percent
        inputted_frame[
            f"{p}_{on_measure.name}_{on_type.name}"
        ] = inputted_frame[on_measure.name] / getattr(
            top_line, on_measure.name
        )
        return inputted_frame
        pass

    @staticmethod
    def _generate_performance_summary_cluster(
        this_result: OneThreeFiveResult,
        top_line_node: PvmNodeEvaluatable,
        dimn: pd.DataFrame,
        investment_mapping_info: pd.DataFrame,
    ) -> PerformanceConcentrationResultCluster:
        """_summary_

        Args:
            this_result (OneThreeFiveResult): _description_
            top_line_node (PvmNodeEvaluatable): used for computing % of total gain, etc
        """
        measures = PvmNodeEvaluatable.PvmEvaluationType
        # first get all deals
        top_deal_measures = [
            measures.cost,
            measures.unrealized_value,
            measures.total_value,
            measures.pnl,
            measures.moic,
            measures.irr,
        ]
        top_deals = this_result.top_5_atoms.children
        top_deals_df = simple_display_df_from_list(
            top_deals, top_deal_measures
        )
        other = this_result.other_minus_top_5_atoms
        top_deals_df = pd.concat(
            [top_deals_df, df_evaluate(other, top_deal_measures)]
        )
        top_deals_df.reset_index(inplace=True, drop=True)
        top_deals_df = PerformanceConcentrationRenderer.compute_percent_of(
            top_deals_df,
            this_result.base_item,
            on_type=PerformanceConcentrationRenderer._OnTheFlyComputedColumnType.InGroup,
            on_measure=measures.pnl,
        )
        top_deals_df = PerformanceConcentrationRenderer.compute_percent_of(
            top_deals_df,
            top_line_node,
            on_type=PerformanceConcentrationRenderer._OnTheFlyComputedColumnType.OnTopLine,
            on_measure=measures.pnl,
        )

        assert top_deals_df is not None

    def render(self):
        top_line = self.breakout.All.base_item

        def _apply_and_gen(
            _item: OneThreeFiveResult,
        ) -> PerformanceConcentrationResultCluster:
            output = PerformanceConcentrationRenderer._generate_performance_summary_cluster(
                _item,
                top_line_node=top_line,
                dimn=self.position_dimn,
                investment_mapping_info=self.position_to_investment_mapping,
            )
            # now append and cleanup
            return output

        all = _apply_and_gen(self.breakout.All)
        realized = _apply_and_gen(self.breakout.Realized)
        unrealized = _apply_and_gen(self.breakout.Unrealized)
        return PerformanceConcentrationAll(
            All_Deals=all,
            Realized_Deals=realized,
            Unrealized_Deals=unrealized,
        )
