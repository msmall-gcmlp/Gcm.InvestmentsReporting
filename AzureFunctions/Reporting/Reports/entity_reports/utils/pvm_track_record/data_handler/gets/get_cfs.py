from typing import List
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
from gcm.Dao.DaoSources import DaoSource


def get_cfs(ids: List[int], cf_type="Position"):
    runner: DaoRunner = Scenario.get_attribute("dao")
    assert runner is not None

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"{cf_type}Id", ids)
        # TODO: handle aggregate_interval and scenario for projected CFs
        return query

    p = {
        "table": f"{cf_type}cashflows",
        "schema": "PvmTrackRecord",
        "operation": lambda query, items: oper(query, items),
    }
    df = runner.execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    return df
