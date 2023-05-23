from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from functools import cached_property
from ..cashflows import PvmCashflows
import pandas as pd
from pyxirr import xirr
from enum import Enum, auto


class PvmPerformanceResultsBase(object):
    def __init__(
        self,
        cfs: PvmCashflows,
        aggregate_interval: AggregateInterval = AggregateInterval.ITD,
    ):
        self.cfs = cfs
        self.aggregate_interval = aggregate_interval

    @property
    def performance_results_count(self) -> int:
        return 1

    @cached_property
    def irr(self) -> float:
        dates = self.cfs.cfs[
            PvmCashflows.CashflowColumns.CashflowDate.name
        ]
        vals = self.cfs.cfs[PvmCashflows.CashflowColumns.Amount.name]
        guess = 0.1 if self.pnl > 0 else -0.1
        return xirr(dates, vals, guess=guess)

    @cached_property
    def moic(self) -> float:
        return 1.0 + (self.pnl / abs(self.cost))

    @cached_property
    def loss_ratio(self) -> float:
        if self.pnl < 0.0:
            return self.pnl / abs(self.cost)
        return 0.0

    @property
    def pnl(self) -> float:
        return self.cfs.sum(self.cfs.cfs)

    @property
    def cost(self) -> float:
        return self.cfs.sum(self.cfs.T_Cfs)

    @property
    def distributions(self) -> float:
        return self.cfs.sum(self.cfs.D_Cfs)

    @property
    def nav(self) -> float:
        return self.cfs.sum(self.cfs.R_Cfs)

    # ALIAS
    @property
    def realized_value(self) -> float:
        return self.distributions

    # ALIAS
    @property
    def unrealized_value(self) -> float:
        return self.nav

    # ALIAS
    @property
    def tvpi(self) -> float:
        return self.moic

    class Measures(Enum):
        irr = auto()
        moic = auto()
        loss_ratio = auto()
        pnl = auto()
        cost = auto()
        distributions = auto()
        nav = auto()
        realized_value = auto()
        unrealized_value = auto()
        tvpi = auto()
        performance_results_count = auto()

    def get_measure(self, measure: Measures):
        attr = getattr(self, measure.name, None)
        return attr

    def measure_cols(self):
        cols = [x for x in PvmPerformanceResultsBase.Measures]
        return cols

    def to_df(self) -> pd.DataFrame:
        # TODO: make more dynamic
        df_dict = {}
        for i in self.measure_cols():
            attr = self.get_measure(i)
            if attr is not None:
                df_dict[i.name] = [attr]
        return pd.DataFrame(df_dict)
