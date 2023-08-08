import pandas as pd
from gcm.inv.models.pvm.underwriting_analytics.perf_1_3_5 import (
    OneThreeFiveResult,
    PvmNodeEvaluatable,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_item.to_df import (
    simple_display_df_from_evaluatable as df_evaluate,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.df_utils import (
    ATOMIC_COUNT,
    compute_percent_of,
    OnTheFlyComputedColumnType,
    generate_percent_column,
    atomic_node_count,
)
from gcm.inv.utils.pvm.node import PvmNodeBase
from typing import List, Tuple
from ..utils import enhanced_display_name


class BaseRenderer:
    def __init__(self) -> None:
        pass

    _PERCENT_OF_TOTAL = PvmNodeEvaluatable.PvmEvaluationType.pnl

    Evaluated_Columns = [
        PvmNodeEvaluatable.PvmEvaluationType.irr,
        PvmNodeEvaluatable.PvmEvaluationType.moic,
        _PERCENT_OF_TOTAL,
    ]

    @classmethod
    def _reference_columns(cls) -> List[str]:
        return [PvmNodeBase._DISPLAY_NAME]

    @classmethod
    def _evaluated_columns_to_show_in_df(cls):
        return [
            x.name
            for x in cls.Evaluated_Columns
            if x != cls._PERCENT_OF_TOTAL
        ]

    @classmethod
    def _final_column_list(cls):
        reference_columns = cls._reference_columns()
        evaluated_columns = cls._evaluated_columns_to_show_in_df()
        percentage_columns = [
            OnTheFlyComputedColumnType.InGroup,
            OnTheFlyComputedColumnType.OnTopLine,
        ]
        percentage_columns = [
            generate_percent_column(x, cls._PERCENT_OF_TOTAL)
            for x in percentage_columns
        ]
        return reference_columns + evaluated_columns + percentage_columns

    @classmethod
    def render_with_final_columns(
        cls, frame: pd.DataFrame
    ) -> pd.DataFrame:
        final_col_list = cls._final_column_list()
        for k in final_col_list:
            if k not in frame.columns:
                frame[k] = None
        return frame[final_col_list]

    @classmethod
    def generate_updated_diplay_name(
        cls, df: pd.DataFrame
    ) -> pd.DataFrame:
        df = enhanced_display_name(df)
        return df

    @classmethod
    def _generate_breakout_tables(cls, this_result: OneThreeFiveResult):
        measures = cls.Evaluated_Columns
        top_deals = this_result.atomic_one_three_five_other_result
        one_three_five_other = []
        for k, v in top_deals.items():
            df = df_evaluate(v, measures)
            if k < 0:
                # is 'other'
                df[ATOMIC_COUNT] = int(atomic_node_count(v))
            one_three_five_other.append(df)
        one_three_five_other = pd.concat(one_three_five_other)
        one_three_five_other.reset_index(inplace=True, drop=True)
        return one_three_five_other

    @classmethod
    def _generate_total_tables(cls, this_result: OneThreeFiveResult):
        total_df = df_evaluate(
            this_result.base_item, cls.Evaluated_Columns
        )
        total_df[ATOMIC_COUNT] = int(
            atomic_node_count(this_result.base_item)
        )
        return total_df

    @classmethod
    def _generate_percentage_breakouts(
        cls,
        this_result: OneThreeFiveResult,
        top_line_node: PvmNodeEvaluatable,
        breakout: pd.DataFrame,
        total_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        evaluate_percentages = [
            (OnTheFlyComputedColumnType.InGroup, this_result.base_item),
            (OnTheFlyComputedColumnType.OnTopLine, top_line_node),
        ]
        for j in evaluate_percentages:
            breakout = compute_percent_of(
                breakout,
                top_line=j[1],
                on_type=j[0],
                on_measure=cls._PERCENT_OF_TOTAL,
            )
            total_df = compute_percent_of(
                total_df,
                top_line=j[1],
                on_type=j[0],
                on_measure=cls._PERCENT_OF_TOTAL,
            )
        return [breakout, total_df]

    @classmethod
    def _extend_breakout_table(
        cls,
        this_result: OneThreeFiveResult,
        breakout: pd.DataFrame,
        position_dimn: pd.DataFrame,
        investment_mapping: pd.DataFrame,
    ):
        return breakout

    @classmethod
    def generate(
        cls,
        this_result: OneThreeFiveResult,
        top_line_node: PvmNodeEvaluatable,
        position_dimn: pd.DataFrame,
        investment_mapping: pd.DataFrame,
    ) -> "BaseRenderer":
        # first get all deals
        breakout = cls._generate_breakout_tables(this_result=this_result)
        breakout = cls._extend_breakout_table(
            this_result=this_result,
            breakout=breakout,
            position_dimn=position_dimn,
            investment_mapping=investment_mapping,
        )
        total_df = cls._generate_total_tables(this_result=this_result)
        [breakout, total_df] = cls._generate_percentage_breakouts(
            this_result=this_result,
            top_line_node=top_line_node,
            breakout=breakout,
            total_df=total_df,
        )
        breakout = cls.generate_updated_diplay_name(breakout)
        breakout = cls.render_with_final_columns(breakout)
        total_df = cls.generate_updated_diplay_name(total_df)
        total_df = cls.render_with_final_columns(total_df)
        return cls(breakout, total_df)
