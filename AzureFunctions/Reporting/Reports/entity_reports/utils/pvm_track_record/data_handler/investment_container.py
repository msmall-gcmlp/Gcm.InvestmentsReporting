from abc import abstractproperty, abstractmethod
import pandas as pd
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from .gross_atom import GrossAttributionAtom


class InvestmentContainerBase(object):
    def __init__(self):
        pass

    @abstractproperty
    def name(self) -> str:
        raise NotImplementedError()

    @abstractproperty
    def investment_cashflows(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def investment_dimn(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def investment_fact(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def position_cashflows(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def position_dimn(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def position_fact(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def asset_dimn(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_atom_level_performance_result_cache(
        self, agg: AggregateInterval
    ):
        raise NotImplementedError()

    @abstractproperty
    def gross_atom(self) -> GrossAttributionAtom:
        return NotImplementedError()
