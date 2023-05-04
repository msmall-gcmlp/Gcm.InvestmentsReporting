import pandas as pd
from typing import List
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
from gcm.Dao.DaoSources import DaoSource


def get_position_map(
    input_df: pd.DataFrame, position_list: List[int]
) -> pd.DataFrame:
    runner: DaoRunner = Scenario.get_attribute("dao")

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"PositionId", position_list)
        query = query.with_entities(
            item.PositionId, item.AssetId, item.InvestmentId
        )
        return query

    p = {
        "table": "PositionDimn",
        "schema": "PvmTrackRecord",
        "operation": lambda query, items: oper(query, items),
    }
    df = runner.execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    final = pd.merge(input_df, df, on=["PositionId"], how="left")
    return final
