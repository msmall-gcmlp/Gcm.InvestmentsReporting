from abc import abstractproperty
import pandas as pd


class InvestmentContainerBase(object):
    def __init__(self):
        pass

    @abstractproperty
    def net_cashflows(self) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def gross_cashflows(self) -> pd.DataFrame:
        raise NotImplementedError()
