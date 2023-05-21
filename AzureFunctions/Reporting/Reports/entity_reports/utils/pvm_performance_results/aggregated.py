from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from AzureFunctions.Reporting.Reports.entity_reports.utils.cashflows import (
    PvmCashflows,
)
from . import PvmPerformanceResultsBase, PvmCashflows, AggregateInterval
from functools import cached_property
import pandas as pd


class PvmAggregatedPerformanceResults(PvmPerformanceResultsBase):
    def __init__(
        self,
        name: str,
        components: dict[str, PvmPerformanceResultsBase],
        aggregate_interval: AggregateInterval = AggregateInterval.ITD,
    ):
        self.name = name
        self.components = components
        cfs: PvmCashflows = None
        if all([components[x].cfs is not None for x in components]):
            cf_objects = [
                components[x].cfs for x in components if x is not None
            ]
            cfs = PvmCashflows(pd.concat([x.cfs for x in cf_objects]))
        super().__init__(cfs, aggregate_interval)

    @cached_property
    def pnl(self):
        return sum([self.components[x].pnl for x in self.components])

    @cached_property
    def cost(self):
        return sum([self.components[x].cost for x in self.components])

    @cached_property
    def distrutions(self):
        return sum(
            [self.components[x].distrutions for x in self.components]
        )

    def to_df(self, include_children=True) -> pd.DataFrame:
        cache = []
        df = super().to_df()
        df["Name"] = self.name
        cache.append(df)
        if include_children:
            for k in self.components:
                item = self.components[k]
                item_df = item.to_df()
                item_df["Name"] = k
                cache.append(item_df)
        final = pd.concat(cache)
        return final
