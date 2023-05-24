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
)
import pandas as pd
from ...report_names import ReportNames
import copy
from ..investment_reports.pvm_investment_trackrecord_report import (
    PvmInvestmentTrackRecordReport,
)
from ..utils.pvm_track_record.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
)
from functools import cached_property
from typing import List
from ..utils.pvm_performance_results.attribution import (
    PvmTrackRecordAttribution,
)
from ..utils.pvm_track_record.renderers.position_summary import (
    PositionSummarySheet,
)
from ..utils.pvm_track_record.renderers.position_concentration_1_3_5 import (
    PositionConcentration,
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
            # path=["Underwriting_Packet_Template.xlsx"]
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

    @property
    def pvm_perfomance_results(self) -> PvmTrackRecordAttribution:
        investments = [
            self.children_reports[i].investment_handler
            for i in self.children_reports
        ]
        return PvmTrackRecordAttribution(investments)

    @cached_property
    def children_reports(
        self,
    ) -> dict[str, PvmInvestmentTrackRecordReport]:
        cach_dict = {}
        for g, n in self.children.groupby(EntityStandardNames.EntityName):
            meta = copy.deepcopy(self.report_meta)
            meta.entity_domain = self.child_type
            meta.entity_info = n
            this_report = PvmInvestmentTrackRecordReport(
                meta, self.manager_name
            )
            cach_dict[g] = this_report
        return cach_dict

    def assign_components(self) -> List[ReportWorkBookHandler]:
        formatted = PositionSummarySheet(self)
        details = formatted.to_worksheet()
        performance_concentration = PositionConcentration(self)
        concentration = performance_concentration.to_worksheet()
        report_name = self.manager_name
        final_list = [
            ReportWorkBookHandler(
                report_name,
                self.excel_template_location,
                [details, concentration],
            )
        ]
        for k, v in self.children_reports.items():
            components: List[ReportWorkBookHandler] = v.components
            final_list = final_list + components
        return final_list
