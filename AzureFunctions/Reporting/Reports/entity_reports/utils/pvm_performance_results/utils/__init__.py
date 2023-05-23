from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from AzureFunctions.Reporting.Reports.entity_reports.utils.pvm_performance_results import (
    AggregateInterval,
)
from . import AggregateInterval
from ..aggregated import PvmAggregatedPerformanceResults
import pandas as pd


def aggregate_other(
    amount: int, df: pd.DataFrame, interval: AggregateInterval
):
    other_df = df.iloc[amount:]
    if other_df.shape[0] > 0:
        other_final = {}
        in_count = 1
        for i, s in other_df.iterrows():
            other_final[f"Top {in_count}"] = s["obj"]
            in_count = in_count + 1
        other = PvmAggregatedPerformanceResults(
            f"Other [-{amount}]",
            other_final,
            interval,
        )
        return other
    return None


def get_concentration(
    full_expansion,
    aggregate_interval: AggregateInterval,
    top: int = 1,
    ascending=False,
    return_other=False,
) -> tuple[
    PvmAggregatedPerformanceResults,
    PvmAggregatedPerformanceResults,
]:
    pnl_list = []
    item_list = []
    for i in full_expansion:
        pnl_list.append(full_expansion[i].pnl)
        item_list.append(full_expansion[i])
    df = pd.DataFrame({"pnl": pnl_list, "obj": item_list})
    df.sort_values(by="pnl", ascending=ascending, inplace=True)
    sorted = df.head(top)
    final = {}
    in_count = 1
    for i, s in sorted.iterrows():
        final[f"Top {in_count}"] = s["obj"]
        in_count = in_count + 1
    # now get other if return_other
    other = None
    if return_other:
        other = aggregate_other(top, df, aggregate_interval)
    return [
        PvmAggregatedPerformanceResults(
            f"Top {top}",
            final,
            aggregate_interval,
        ),
        other,
    ]
