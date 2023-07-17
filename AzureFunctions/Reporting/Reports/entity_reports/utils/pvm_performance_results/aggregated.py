from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ....entity_reports.utils.cashflows import (
    PvmCashflows,
)
from . import PvmPerformanceResultsBase
import pandas as pd
from functools import cached_property


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
            inputted = (
                PvmCashflows.empty_df()
                if len(cf_objects) == 0
                else pd.concat([x.cfs for x in cf_objects])
            )
            cfs = PvmCashflows(inputted)
        super().__init__(cfs, aggregate_interval)

    @classmethod
    def get_lowest_expansion_types(cls):
        return [PvmPerformanceResultsBase]

    @classmethod
    def get_highest_expansion_types(cls):
        _highest = [cls]
        return _highest

    @classmethod
    def _expand_to_lowest(
        cls,
        results: "PvmAggregatedPerformanceResults",
    ) -> dict[str, PvmPerformanceResultsBase]:
        items = {}
        for c in results.components:
            # TODO: do better subtype check
            item = results.components[c]
            for i in cls.get_highest_expansion_types():
                if issubclass(type(item), i):
                    expanded_items = cls._expand_to_lowest(item)
                    items = items | expanded_items
                else:
                    for i in cls.get_lowest_expansion_types():
                        if issubclass(type(item), i):
                            items[c] = item
                        else:
                            raise NotImplementedError()
        return items

    @cached_property
    def full_expansion(self) -> dict[str, PvmPerformanceResultsBase]:
        return self.__class__._expand_to_lowest(self)

    @property
    def full_expanded_performance_results_count(self) -> int:
        keys = list(self.full_expansion.keys())
        return len(keys)

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
    def realized_value(self):
        return sum(
            [self.components[x].realized_value for x in self.components]
        )

    @cached_property
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

    def to_df(self) -> pd.DataFrame:
        df = super().to_df()
        df["Name"] = self.name
        return df
