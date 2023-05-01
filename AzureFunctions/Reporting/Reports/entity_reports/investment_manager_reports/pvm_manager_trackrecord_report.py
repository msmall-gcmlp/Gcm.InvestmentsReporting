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
from ..PvmTrackRecord.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
)

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


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
    def available_metas(cls):
        base = super().available_metas()
        base.entity_groups = [
            EntityDomainTypes.InvestmentManager,
        ]
        return base

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
        child_type = EntityDomainTypes.Investment
        structure = self.manager_handler.manager_hierarchy_structure
        children = structure.get_entities_directly_related_by_name(
            child_type
        )
        final_list = [wb_handler]
        for g, n in children.groupby(EntityStandardNames.EntityName):
            meta = copy.deepcopy(self.report_meta)
            meta.entity_domain = child_type
            meta.entity_info = n
            this_report = PvmInvestmentTrackRecordReport(
                meta, self.manager_name
            )
            wb = ReportWorkBookHandler(
                g,
                this_report.components,
                this_report.excel_template_location,
            )
            final_list.append(wb)
        return final_list
