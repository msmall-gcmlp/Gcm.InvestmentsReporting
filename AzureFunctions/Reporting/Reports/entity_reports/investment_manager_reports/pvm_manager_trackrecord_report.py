from ..investment_reports.pvm_track_record import (
    BasePvmTrackRecordReport,
)
from ....core.report_structure import (
    ReportMeta,
    EntityStandardNames,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
)
from ....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
    ReportWorksheet,
)
import pandas as pd
from ...report_names import ReportNames
from functools import cached_property
from typing import List
from ..investment_reports.pvm_track_record.render_135 import (
    OneThreeFiveRenderer,
    TEMPLATE as Template_135,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider import (
    PvmEvaluationProvider,
)
from gcm.inv.models.pvm.underwriting_analytics.perf_1_3_5 import (
    generate_realized_unrealized_all_performance_breakout,
)

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-06-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class PvmManagerTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmManagerTrackRecordReport, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmManagerTrackRecordTemplate.xlsx"],
        )

    @cached_property
    def manager_name(self):
        manager_name: pd.DataFrame = self.report_meta.entity_info[
            [EntityStandardNames.EntityName]
        ].drop_duplicates()
        manager_name = "_".join(
            manager_name[EntityStandardNames.EntityName].to_list()
        )
        return manager_name

    @classmethod
    def level(cls):
        return EntityDomainTypes.InvestmentManager

    @property
    def child_type(self):
        return EntityDomainTypes.Investment

    @cached_property
    def children(self) -> pd.DataFrame:
        structure = self.manager_handler.manager_hierarchy_structure
        children = structure.get_entities_directly_related_by_name(
            self.child_type
        )
        return children

    def generate_135_tables(self) -> ReportWorksheet:
        positions: PvmEvaluationProvider = (
            self.node_provider.position_tr_node_provider
        )
        realized_breakout = (
            generate_realized_unrealized_all_performance_breakout(
                self.node_provider.position_tr_node_provider
            )
        )
        dimns = positions.atomic_dimensions
        position_map = self.position_to_investment_breakout
        item = OneThreeFiveRenderer(
            breakout=realized_breakout,
            position_to_investment_mapping=position_map,
            position_dimn=dimns,
        )
        return item.render()

    @property
    def investments(self) -> List[str]:
        c = self.children
        assert c is not None
        return list(
            c[EntityStandardNames.EntityName].dropna().drop_duplicates()
        )

    def assign_components(self) -> List[ReportWorkBookHandler]:
        return [
            ReportWorkBookHandler(
                self.manager_name,
                Template_135,
                [self.generate_135_tables()],
            )
        ]
