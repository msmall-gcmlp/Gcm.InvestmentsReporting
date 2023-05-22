from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from AzureFunctions.Reporting.Reports.entity_reports.utils.cashflows import (
    PvmCashflows,
)
from . import PvmPerformanceResultsBase, PvmCashflows, AggregateInterval
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

    @property
    def pnl(self):
        return sum([self.components[x].pnl for x in self.components])

    @property
    def cost(self):
        return sum([self.components[x].cost for x in self.components])

    @property
    def distrutions(self):
        return sum(
            [self.components[x].distributions for x in self.components]
        )
    
    @property
    def loss_ratio(self) -> float:
        total_cost_tracker = 0.0
        loss_tracker = 0.0
        for i in self.components:
            item = self.components[i]
            loss_ratio = item.loss_ratio
            cost_amount = item.cost
            loss_amount = loss_ratio * cost_amount
            total_cost_tracker = total_cost_tracker + cost_amount
            loss_tracker = loss_tracker + loss_amount
        return loss_tracker / total_cost_tracker

    
    @staticmethod
    def _expand(
        results: "PvmAggregatedPerformanceResults",
    ) -> dict[str, PvmPerformanceResultsBase]:
        items = {}
        for c in results.components:
            # TODO: do better subtype check
            item = results.components[c]
            if type(item) == PvmAggregatedPerformanceResults:
                expanded_items = (
                    PvmAggregatedPerformanceResults._expand(
                        item
                    )
                )
                items = items | expanded_items
            elif type(item) == PvmPerformanceResultsBase:
                items[c] = item
            else:
                raise NotImplementedError()
        return items
    

    def to_df(self) -> pd.DataFrame:
        df = super().to_df()
        df["Name"] = self.name
        return df
