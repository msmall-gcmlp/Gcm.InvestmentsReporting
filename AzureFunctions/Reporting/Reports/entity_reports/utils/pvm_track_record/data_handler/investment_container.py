from abc import abstractproperty
import pandas as pd


class InvestmentContainerBase(object):
    def __init__(self):
        pass

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
    def asset_dimn(self) -> pd.DataFrame:
        raise NotImplementedError()
