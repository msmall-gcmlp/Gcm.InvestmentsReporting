from ....core.report_structure import (
    ReportMeta,
    EntityStandardNames,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
)
from ....core.components.report_table import ReportTable
from ....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
    ReportWorksheet,
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
from ..utils.pvm_track_record.analytics.attribution import (
    PvmTrackRecordAttribution,
)
from ..utils.pvm_track_record.analytics.attribution import (
    get_mgr_rpt_dict,
    get_perf_concentration_rpt_dict,
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
        # DT: filtering below because funky stuff going on with get_entities_directly_related_by_name
        children = children[children.SourceName == "PVM.TR.Investment.Id"]
        return children

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
        entity_name: str = self.report_meta.entity_info[
            "EntityName"
        ].unique()[0]
        # DT: need to add tables here
        # as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        # domain = self.report_meta.entity_domain
        # entity_info = self.report_meta.entity_info
        tr_json = get_mgr_rpt_dict(
            manager_attrib=self.manager_handler.manager_attrib,
            fund_attrib=self.manager_handler.investment_attrib,
            deal_attrib=self.manager_handler.position_attrib,
            deal_cf=self.manager_handler.position_cf,
        )

        tr_tables: List[ReportTable] = []
        for k, v in tr_json.items():
            this_table = ReportTable(k, v)
            tr_tables.append(this_table)

        worksheets = [
            ReportWorksheet(
                "Manager TR",
                report_tables=tr_tables,
                render_params=ReportWorksheet.ReportWorkSheetRenderer(
                    trim_region=[x.component_name for x in tr_tables]
                ),
            )
        ]

        perf_concen_json = get_perf_concentration_rpt_dict(
            deal_attrib=self.manager_handler.position_attrib,
            deal_cf=self.manager_handler.position_cf,
        )

        perf_concen_tbls: List[ReportTable] = []
        for k, v in perf_concen_json.items():
            this_table = ReportTable(k, v)
            perf_concen_tbls.append(this_table)

        worksheets.append(
            ReportWorksheet(
                "Performance Concentration",
                report_tables=perf_concen_tbls,
                render_params=ReportWorksheet.ReportWorkSheetRenderer(
                    trim_region=[
                        x.component_name for x in perf_concen_tbls
                    ]
                ),
            )
        )

        all_investments = [
            self.children_reports[k].investment_handler
            for k in self.children_reports
        ]
        attribution = PvmTrackRecordAttribution(all_investments)
        attribution.net_performance_results()

        name = f"ManagerTR_{self.manager_name}"
        wb_handler = ReportWorkBookHandler(
            name,
            report_sheets=worksheets,
            template_location=self.excel_template_location,
        )

        final_list = [wb_handler]
        for k, v in self.children_reports.items():
            components: List[ReportWorkBookHandler] = v.components
            final_list = final_list + components
        return final_list
