from typing import List
import datetime as dt
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
from gcm.Dao.DaoSources import DaoSource


def get_position_cfs(
    position_ids: List[int],
    as_of_date: dt.date,
    aggregate_interval=AggregateInterval.ITD,
    scenario_name="Base",
):
    runner: DaoRunner = Scenario.get_attribute("dao")
    assert runner is not None

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        return filter_many(query, item, "PositionId", position_ids)

    p = {
        "table": "PositionCashflows",
        "schema": "PvmTrackRecord",
        "operation": lambda query, items: oper(query, items),
    }
    df = runner.execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    df = df[df["AsOfDate"] == as_of_date]
    # TODO: handle aggregate_interval and scenario for projected CFs
    return df
