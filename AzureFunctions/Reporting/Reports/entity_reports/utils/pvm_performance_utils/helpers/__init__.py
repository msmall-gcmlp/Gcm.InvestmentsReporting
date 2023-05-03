from gcm.inv.entityhierarchy.EntityDomain.entity_domain import (
    pd,
)
from gcm.inv.utils.DaoUtils.query_utils import filter_many
from typing import List
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
from gcm.Dao.DaoSources import DaoSource
import numpy as np


def get_ilevel_cfs(os_list: List[str]) -> pd.DataFrame:
    runner: DaoRunner = Scenario.get_attribute("dao")
    assert runner is not None

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"OwnerName", os_list)
        # TODO: handle aggregate_interval and scenario for projected CFs
        return query

    p = {
        "table": f"vExtendedCollapsedCashflows",
        "schema": "iLevel",
        "operation": lambda query, items: oper(query, items),
    }
    df = runner.execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    return df


def get_to_usd_fx_rates() -> pd.DataFrame:
    def my_dao_operation(dao, params):
        raw = """select AsOfDt Date, FromCurrcyCd, ToCurrcyCd, Multiplier from analyticsdata.FXFact
                where ToCurrcyCd = 'USD'
                and MonthsForward = 0
                order by FromCurrcyCd, AsOfDt"""
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    runner: DaoRunner = Scenario.get_attribute("dao")
    fx_rates = runner.execute(
        params={},
        source=DaoSource.PubDwh,
        operation=my_dao_operation,
    )
    return fx_rates


def convert_amt_to_usd(df: pd.DataFrame, fx_rates: pd.DataFrame):
    assert len(
        df[
            (df.TransactionDate.isin(fx_rates.Date))
            & (df.BaseCurrency.isin(fx_rates.FromCurrcyCd))
        ]
    ) == len(df[df.BaseCurrency != "USD"])

    df_fx = df.merge(
        fx_rates,
        how="left",
        left_on=["BaseCurrency", "TransactionDate"],
        right_on=["FromCurrcyCd", "Date"],
    )
    assert len(df) == len(df_fx)
    df_fx.Multiplier = np.where(
        df_fx.Multiplier.isnull(), 1, df_fx.Multiplier
    )
    df_fx["BaseAmount"] = df_fx.BaseAmount * df_fx.Multiplier

    result = df_fx[df.columns]
    return result
