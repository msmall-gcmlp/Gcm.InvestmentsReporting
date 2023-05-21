from enum import Enum, auto
import pandas as pd


class PvmCashflows(object):
    def __init__(self, cfs: pd.DataFrame):
        PvmCashflows.validate(cfs)
        self.cfs = cfs

    @staticmethod
    def empty_df() -> pd.DataFrame:
        df = pd.DataFrame()
        required_col_names = [e.name for e in PvmCashflows.CashflowColumns]
        for e in required_col_names:
            df[e] = None
        return df

    class CF_Type(Enum):
        T = auto()  # takedown
        D = auto()  # distribution
        R = auto()  # residual value
        Other = auto()

    class CashflowColumns(Enum):
        CashflowDate = auto()
        CashflowType = auto()
        Currency = auto()
        Amount = auto()
        AsOfDate = auto()
        AggregateIntervalName = auto()
        ScenarioName = auto()

    @staticmethod
    def validate(cfs: pd.DataFrame):
        columns = cfs.columns
        required_col_names = [e.name for e in PvmCashflows.CashflowColumns]
        for r in required_col_names:
            if r not in columns:
                raise RuntimeError()
        items_in_type = [e.name for e in PvmCashflows.CF_Type]
        filtered = cfs[
            ~cfs[PvmCashflows.CashflowColumns.CashflowType.name].isin(
                items_in_type
            )
        ]
        if filtered.shape[0] != 0:
            raise RuntimeError()

    @staticmethod
    def filter_on_cf_type(
        cfs: pd.DataFrame, cf_type: CF_Type
    ) -> pd.DataFrame:
        return cfs[
            cfs[PvmCashflows.CashflowColumns.CashflowType.name]
            == cf_type.name
        ]

    @property
    def D_Cfs(self) -> pd.DataFrame:
        return PvmCashflows.filter_on_cf_type(
            self.cfs, PvmCashflows.CF_Type.D
        )

    @property
    def T_Cfs(self) -> pd.DataFrame:
        return PvmCashflows.filter_on_cf_type(
            self.cfs, PvmCashflows.CF_Type.T
        )

    @property
    def R_Cfs(self) -> pd.DataFrame:
        return PvmCashflows.filter_on_cf_type(
            self.cfs, PvmCashflows.CF_Type.R
        )

    @property
    def Other_Cfs(self) -> pd.DataFrame:
        return PvmCashflows.filter_on_cf_type(
            self.cfs, PvmCashflows.CF_Type.Other
        )

    @staticmethod
    def sum(cfs: pd.DataFrame, col=CashflowColumns.Amount) -> float:
        return cfs[col.name].sum()
