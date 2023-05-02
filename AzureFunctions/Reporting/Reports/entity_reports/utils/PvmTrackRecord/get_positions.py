from .....core.entity_handler import HierarchyHandler
from .....core.report_structure import (
    EntityDomainTypes,
    Standards as EntityDomainStandards,
    DaoSource,
)
import pandas as pd
from gcm.inv.scenario import Scenario
import json
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)


def get_positions(
    struct: HierarchyHandler, entity_info: pd.DataFrame
) -> pd.DataFrame:
    runner = Scenario.get_attribute("dao")
    nodes = (
        entity_info[EntityDomainStandards.NodeId]
        .drop_duplicates()
        .to_list()
    )
    assets = struct.get_entities_directly_related_by_name(
        EntityDomainTypes.Asset, starting_node_id=nodes, down=True
    )
    assert assets is not None

    positions = [
        int(json.loads(x)["Position_Id"])
        for x in assets["EdgeInfo"].drop_duplicates().to_list()
    ]

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        return filter_many(query, item, "PositionId", positions)

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
    return df
