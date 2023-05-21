from gcm.inv.dataprovider.entity_provider.hierarchy_controller.hierarchy_handler import (
    HierarchyHandler,
)
from .......core.report_structure import (
    EntityDomainTypes,
    Standards as EntityDomainStandards,
    DaoRunner,
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


def get_assets(struct: HierarchyHandler, entity_info: pd.DataFrame):
    nodes = (
        entity_info[EntityDomainStandards.NodeId]
        .drop_duplicates()
        .to_list()
    )
    assets = struct.get_entities_directly_related_by_name(
        EntityDomainTypes.Asset, starting_node_id=nodes, down=True
    )
    return assets


def get_positions(assets: pd.DataFrame) -> pd.DataFrame:
    runner: DaoRunner = Scenario.get_attribute("dao")
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
