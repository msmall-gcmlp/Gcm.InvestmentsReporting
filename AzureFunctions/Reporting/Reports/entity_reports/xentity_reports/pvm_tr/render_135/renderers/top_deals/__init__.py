import pandas as pd
from gcm.inv.models.pvm.underwriting_analytics.perf_1_3_5 import (
    OneThreeFiveResult,
    PvmNodeEvaluatable,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_item.to_df import (
    simple_display_df_from_evaluatable as df_evaluate,
    simple_display_df_from_list,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.df_utils import (
    nodes_to_materialized_dict,
    ATOMIC_COUNT,
    atomic_node_count,
)
from gcm.inv.utils.pvm.node import PvmNodeBase
from typing import List
from ....utils import enhanced_display_name
from ....base_render import BaseRenderer


class TopDeals_And_Total(BaseRenderer):
    def __init__(
        self,
        Deal_Summary_Top_N_Deals_And_Other: pd.DataFrame,
        Deal_Summary_Total: pd.DataFrame,
    ) -> None:
        self.Deal_Summary_Top_N_Deals_And_Other = (
            Deal_Summary_Top_N_Deals_And_Other
        )
        self.Deal_Summary_Total = Deal_Summary_Total

    Evaluated_Columns = [
        PvmNodeEvaluatable.PvmEvaluationType.cf_implied_duration,
        PvmNodeEvaluatable.PvmEvaluationType.cost,
        PvmNodeEvaluatable.PvmEvaluationType.unrealized_value,
        PvmNodeEvaluatable.PvmEvaluationType.total_value,
        PvmNodeEvaluatable.PvmEvaluationType.pnl,
        PvmNodeEvaluatable.PvmEvaluationType.moic,
        PvmNodeEvaluatable.PvmEvaluationType.irr,
    ]
    _PERCENT_OF_TOTAL = PvmNodeEvaluatable.PvmEvaluationType.pnl

    _POSTION_DIMN = "PositionDimn"
    _INVESTMENT_DIMN = "InvestmentDimn"
    _investment_name = "InvestmentName"

    _REQUIRED_DEAL_REFERENCE_DATA = [
        ("InvestmentDate", _POSTION_DIMN),
        ("ExitDate", _POSTION_DIMN),
        (_investment_name, _INVESTMENT_DIMN),
    ]

    @classmethod
    def _evaluated_columns_to_show_in_df(cls):
        return [x.name for x in cls.Evaluated_Columns]

    @staticmethod
    def _get_deal_reference_data_table(
        top_deals: List[PvmNodeEvaluatable],
        position_dimn: pd.DataFrame,
        investment_mapping: pd.DataFrame,
    ) -> pd.DataFrame:
        df = nodes_to_materialized_dict(
            top_deals, include_atom_count=False
        )
        position_merge_columns = [
            x for x in df.columns if x in position_dimn.columns
        ]
        req = TopDeals_And_Total._REQUIRED_DEAL_REFERENCE_DATA
        select_position_dimn = [
            x[0] for x in req if x[1] == TopDeals_And_Total._POSTION_DIMN
        ]
        top_deals_df = pd.merge(
            df,
            position_dimn[position_merge_columns + select_position_dimn],
            on=position_merge_columns,
            how="left",
        )

        investment_merge_columns = [
            x for x in top_deals_df if x in investment_mapping.columns
        ]
        select_invesment_name = [
            x[0]
            for x in req
            if x[1] == TopDeals_And_Total._INVESTMENT_DIMN
        ]
        top_deals_df = pd.merge(
            top_deals_df,
            investment_mapping[
                investment_merge_columns + select_invesment_name
            ],
            on=investment_merge_columns,
            how="left",
        )

        return top_deals_df

    @classmethod
    def generate_updated_diplay_name(
        cls, df: pd.DataFrame
    ) -> pd.DataFrame:
        if TopDeals_And_Total._investment_name in df.columns:

            def _merge_investment_name(item):
                existing_display_name = str(
                    item[PvmNodeBase._DISPLAY_NAME]
                )
                inv_name = item[TopDeals_And_Total._investment_name]
                if existing_display_name.lower() != "other":
                    return f"{existing_display_name} [{inv_name}]"
                else:
                    return existing_display_name

            df[PvmNodeBase._DISPLAY_NAME] = df.apply(
                lambda x: _merge_investment_name(x), axis=1
            )
        df = enhanced_display_name(df)
        return df

    @classmethod
    def _reference_columns(cls):
        return super()._reference_columns() + [
            x[0]
            for x in cls._REQUIRED_DEAL_REFERENCE_DATA
            if x[0] != cls._investment_name
        ]

    @classmethod
    def _generate_breakout_tables(cls, this_result: OneThreeFiveResult):
        measures = cls.Evaluated_Columns
        top_deals = this_result.top_5_atoms.children
        top_deals_df = simple_display_df_from_list(top_deals, measures)
        other = this_result.other_minus_top_5_atoms
        other_df = df_evaluate(other, measures)
        other_df[ATOMIC_COUNT] = int(atomic_node_count(other))
        top_deals_df = pd.concat([top_deals_df, other_df])
        return top_deals_df

    @classmethod
    def _extend_breakout_table(
        cls,
        this_result: OneThreeFiveResult,
        breakout: pd.DataFrame,
        position_dimn: pd.DataFrame,
        investment_mapping: pd.DataFrame,
    ):
        top_deals = this_result.top_5_atoms.children
        top_deals_ref_data = (
            TopDeals_And_Total._get_deal_reference_data_table(
                top_deals,
                position_dimn=position_dimn,
                investment_mapping=investment_mapping,
            )
        )
        return pd.merge(breakout, top_deals_ref_data, how="left")
