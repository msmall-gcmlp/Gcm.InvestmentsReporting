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
)
import pandas as pd
from ...report_names import ReportNames
import copy
from ..investment_reports.pvm_investment_trackrecord_report import (
    PvmInvestmentTrackRecordReport,
)
from ..utils.PvmTrackRecord.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
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

    @property
    def manager_name(self):
        __name = "__inv_manager_name"
        _ifc = getattr(self, __name, None)
        if _ifc is None:
            manager_name: pd.DataFrame = self.report_meta.entity_info[
                [EntityStandardNames.EntityName]
            ].drop_duplicates()
            manager_name = "_".join(
                manager_name[EntityStandardNames.EntityName].to_list()
            )
            setattr(self, __name, manager_name)
        return getattr(self, __name, None)

    @classmethod
    def level(cls):
        return EntityDomainTypes.InvestmentManager

    @property
    def child_type(self):
        return EntityDomainTypes.Investment

    @property
    def children(self) -> pd.DataFrame:
        __name = "__investment_children"
        _ifc = getattr(self, __name, None)
        if _ifc is None:
            structure = self.manager_handler.manager_hierarchy_structure
            children = structure.get_entities_directly_related_by_name(
                self.child_type
            )
            setattr(self, __name, children)
        return getattr(self, __name, None)

    @property
    def children_reports(
        self,
    ) -> dict[str, PvmInvestmentTrackRecordReport]:
        __name = "__children_report_dict"
        if getattr(self, __name, None) is None:
            cach_dict = {}
            for g, n in self.children.groupby(
                EntityStandardNames.EntityName
            ):
                meta = copy.deepcopy(self.report_meta)
                meta.entity_domain = self.child_type
                meta.entity_info = n
                this_report = PvmInvestmentTrackRecordReport(
                    meta, self.manager_name
                )
                cach_dict[g] = this_report
            setattr(self, __name, cach_dict)
        return getattr(self, __name, None)

    def assign_components(self):
        tables = [
            ReportTable(
                "manager_name",
                pd.DataFrame({"m_name": [self.manager_name]}),
            ),
        ]
        name = f"ManagerTR_{self.manager_name}"
        wb_handler = ReportWorkBookHandler(
            name, tables, self.excel_template_location
        )

        final_list = [wb_handler]
        children_in_order = [
            ReportWorkBookHandler(
                k,
                self.children_reports[k].components,
                self.children_reports[k].excel_template_location,
            )
            for k in self.children_reports
        ]
        final_list = final_list + children_in_order
        return final_list
