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
    atomic_node_count,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.custom_buckets.by_perf_type import (
    PerformanceBucketType,
)
from ....base_render import BaseRenderer


class ReturnDistributions(BaseRenderer):
    def __init__(
        self,
        Return_Distribution_Breakout: pd.DataFrame,
        Return_Distribution_Total: pd.DataFrame,
    ):
        super().__init__()
        self.Return_Distribution_Breakout = Return_Distribution_Breakout
        self.Return_Distribution_Total = Return_Distribution_Total

    _PERCENT_OF_TOTAL = PvmNodeEvaluatable.PvmEvaluationType.cost

    Evaluated_Columns = [
        PvmNodeEvaluatable.PvmEvaluationType.cost_weighted_holding_period,
        PvmNodeEvaluatable.PvmEvaluationType.moic,
        PvmNodeEvaluatable.PvmEvaluationType.irr,
        _PERCENT_OF_TOTAL,
    ]

    @classmethod
    def _generate_breakout_tables(cls, this_result: OneThreeFiveResult):
        measures = cls.Evaluated_Columns
        concentration = this_result.atomic_performance_concentration
        concentration_cache = []
        # retain order
        for v in PerformanceBucketType:
            item = concentration[v]
            count = atomic_node_count(item)
            if count > 0:
                df = df_evaluate(item, measures)
                df[ATOMIC_COUNT] = count
                concentration_cache.append(df)
        concentration_cache = pd.concat(concentration_cache)
        concentration_cache.reset_index(inplace=True, drop=True)
        return concentration_cache
