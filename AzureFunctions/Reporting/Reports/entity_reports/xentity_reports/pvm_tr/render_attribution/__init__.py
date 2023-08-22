from gcm.inv.models.pvm.node_evaluation.evaluation_item import (
    PvmNodeEvaluatable,
)
from ..base_render import (
    RenderTablesRenderer,
    ReportWorksheet,
    BaseRenderer,
)
from ......core.components.report_worksheet import (
    ReportTable,
)
from typing import List
import pandas as pd
from gcm.inv.models.pvm.node_evaluation.evaluation_item.to_df import (
    simple_display_df_from_evaluatable as df_evaluate,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.df_utils import (
    ATOMIC_COUNT,
    atomic_node_count,
)
from gcm.inv.utils.pvm.node.utils import PvmNodeBase
from gcm.Dao.DaoRunner import AzureDataLakeDao

TEMPLATE = AzureDataLakeDao.BlobFileStructure(
    zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
    sources="investmentsreporting",
    entity="exceltemplates",
    path=["AttributionResultsNew.xlsx"],
)


class AttributionTables(BaseRenderer):
    Evaluated_Columns = [
        PvmNodeEvaluatable.PvmEvaluationType.cf_implied_duration,
        PvmNodeEvaluatable.PvmEvaluationType.cost,
        PvmNodeEvaluatable.PvmEvaluationType.realized_value,
        PvmNodeEvaluatable.PvmEvaluationType.unrealized_value,
        PvmNodeEvaluatable.PvmEvaluationType.pnl,
        PvmNodeEvaluatable.PvmEvaluationType.moic,
        PvmNodeEvaluatable.PvmEvaluationType.irr,
    ]
    _PERCENT_OF_TOTAL = PvmNodeEvaluatable.PvmEvaluationType.pnl

    @classmethod
    def _reference_columns(cls) -> List[str]:
        return super()._reference_columns() + [ATOMIC_COUNT]

    @classmethod
    def _evaluated_columns_to_show_in_df(cls):
        return [x.name for x in cls.Evaluated_Columns]

    @classmethod
    def generate(cls, breakout: List[PvmNodeEvaluatable]) -> pd.DataFrame:
        df = []
        for i in breakout:
            this_df = df_evaluate(i, cls.Evaluated_Columns)
            this_df[ATOMIC_COUNT] = int(atomic_node_count(i))
            df.append(this_df)
        df = pd.concat(df)
        df.reset_index(inplace=True, drop=True)
        # NO need to do this because
        # Gavin wants the count as a seperate column
        # df = cls.generate_updated_diplay_name(df)
        df = cls.render_with_final_columns(df)
        df.dropna(axis=1, how="all", inplace=True)
        other = df[df[PvmNodeBase._DISPLAY_NAME].str.upper() == "OTHER"]
        non_other = df[
            df[PvmNodeBase._DISPLAY_NAME].str.upper() != "OTHER"
        ]
        non_other.sort_values(
            by=PvmNodeEvaluatable.PvmEvaluationType.cost.name,
            inplace=True,
            axis=0,
            ascending=True,
        )
        df = pd.concat([non_other, other])
        df.reset_index(inplace=True, drop=True)
        return df


class RenderAttribution(RenderTablesRenderer):
    def __init__(self, breakout: List[PvmNodeEvaluatable]):
        self.breakout = breakout

    GROSS_ATTRIBUTION_TAB = "GrossAttr"

    def _generate_dfs(self) -> dict[str, pd.DataFrame]:
        df = AttributionTables.generate(self.breakout)
        return {"AttributionResults": df}

    def render(self) -> ReportWorksheet:
        d_items = self._generate_dfs()
        to_render = []
        for k, df in d_items.items():
            i = ReportTable(k, df)
            to_render.append(i)
        return ReportWorksheet(
            RenderAttribution.GROSS_ATTRIBUTION_TAB,
            ReportWorksheet.ReportWorkSheetRenderer(),
            to_render,
        )
