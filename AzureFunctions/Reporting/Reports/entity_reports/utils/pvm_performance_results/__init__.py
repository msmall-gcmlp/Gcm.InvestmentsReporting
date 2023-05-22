from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from functools import cached_property
from ..cashflows import PvmCashflows
import pandas as pd
from pyxirr import xirr


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
        dates = self.cfs.cfs[PvmCashflows.CashflowColumns.CashflowDate.name]
        vals = self.cfs.cfs[PvmCashflows.CashflowColumns.Amount.name]
        guess = 0.1 if self.pnl > 0 else -0.1
        return xirr(dates, vals, guess)

    @cached_property
    def moic(self) -> float:
        return 1.0 + (self.pnl / abs(self.cost))

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
