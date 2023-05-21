from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from functools import cached_property
from ..cashflows import PvmCashflows
import pandas as pd


class PvmPerformanceResultsBase(object):
    def __init__(
        self,
        cfs: PvmCashflows,
        aggregate_interval: AggregateInterval = AggregateInterval.ITD,
    ):
        self.cfs = cfs
        self.aggregate_interval = aggregate_interval

    @cached_property
    def irr(self) -> float:
        return 0.0

    @cached_property
    def moic(self) -> float:
        return 1.0 + (self.pnl / self.cost)

    @cached_property
    def tvpi(self) -> float:
        return self.moic

    @cached_property
    def pnl(self) -> float:
        return self.cfs.sum(self.cfs.cfs)

    @cached_property
    def cost(self) -> float:
        return self.cfs.sum(self.cfs.T_Cfs)

    @cached_property
    def distrutions(self) -> float:
        return self.cfs.sum(self.cfs.D_Cfs)

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"IRR": [self.irr], "MOIC": [self.moic], "PNL": [self.pnl]}
        )
