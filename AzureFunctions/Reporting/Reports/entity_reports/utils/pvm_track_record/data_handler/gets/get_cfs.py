from typing import List
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
import numpy as np
import pandas as pd
from gcm.Dao.DaoSources import DaoSource


def get_cfs(ids: List[int], cf_type="Position"):
    runner: DaoRunner = Scenario.get_attribute("dao")
    assert runner is not None

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"{cf_type}Id", ids)
        # TODO: handle aggregate_interval and scenario for projected CFs

        return query

    p = {
        "table": f"v{cf_type}TrackRecord",
        "schema": "PvmTrackRecord",
        "operation": lambda query, items: oper(query, items),
    }
    df: pd.DataFrame = runner.execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    # sigh...
    df.rename(
        columns={
            "TransactionType": "CashflowType",
            "TransactionDate": "CashflowDate",
            "BaseAmount": "Amount",
        },
        inplace=True,
    )
    item = ["InvestmentName", "InvestmentId"]
    if cf_type == "Position":
        item = item + ["AssetName", "AssetId", "PositionId"]
    df = df[
        [
            "AsOfDate",
            "CashflowDate",
            "CashflowType",
            "AsOfDate",
            "Amount",
        ]
        + item
    ]
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Distrib"), "D", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Return of Capital"),
        "D",
        df.CashflowType,
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Proceeds"), "D", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Interest"), "D", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Private Sale"), "D", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Public Sale"), "D", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Dividend"), "D", df.CashflowType
    )

    df.CashflowType = np.where(
        df.CashflowType.str.contains("NAV"), "R", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Current Carrying Value"),
        "R",
        df.CashflowType,
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Investment"), "T", df.CashflowType
    )
    df.CashflowType = np.where(
        df.CashflowType.str.contains("Expenses"), "T", df.CashflowType
    )
    df["Currency"] = "USD"
    df["AggregateIntervalName"] = "ITD"
    df["ScenarioName"] = "Base"
    df.drop_duplicates(inplace=True)
    return df
