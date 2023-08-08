from typing import List
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
import numpy as np
from gcm.Dao.DaoSources import DaoSource


def get_cfs(ids: List[int], cf_type="Position"):
    runner: DaoRunner = Scenario.get_attribute("dao")
    assert runner is not None

    as_of_date: DaoRunner = Scenario.get_attribute("as_of_date")

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"{cf_type}Id", ids)
        # TODO: handle aggregate_interval and scenario for projected CFs
        return query

    p = {
        "table": f"v{cf_type}TrackRecord",
        "schema": "PvmTrackRecord",
        "operation": lambda query, items: oper(query, items),
    }
    df = runner.execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    if cf_type == "Position":
        df["ExitDate"] = np.where(
            df.ExitDate.isnull(), as_of_date, df.ExitDate
        )

    df.TransactionType = np.where(
        df.TransactionType.str.contains("Distrib"), "D", df.TransactionType
    )
    df.TransactionType = np.where(
        df.TransactionType.str.contains("NAV"), "R", df.TransactionType
    )
    assert len(df[~df.TransactionType.isin(["D", "T", "R"])]) == 0

    return df
