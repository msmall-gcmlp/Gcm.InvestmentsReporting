from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
    EntityStandardNames,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
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


# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class PvmManagerTrackRecordReport(ReportStructure):
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

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Once),
            ],
            aggregate_intervals=[AggregateInterval.ITD],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.IC],
                vertical=ReportConsumer.Vertical.PEREI,
            ),
            entity_groups=[
                EntityDomainTypes.InvestmentManager,
            ],
        )

    def assign_components(self):
        tables = [
            ReportTable(
                "MyComponent",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
            ReportTable(
                "MyComponent_2",
                pd.DataFrame({"V1": [1.0, 2.0], "V2": [1.0, 2.0]}),
            ),
        ]
        name = "Test"
        wb_handler = ReportWorkBookHandler(
            name, tables, self.excel_template_location
        )
        child_type = EntityDomainTypes.Investment
        children = self.get_direct_children_of_type(child_type.name)
        final_list = [wb_handler]
        for g, n in children.groupby(EntityStandardNames.EntityName):
            meta = copy.deepcopy(self.report_meta)
            meta.entity_domain = child_type
            meta.entity_info = n
            this_report = PvmInvestmentTrackRecordReport(meta)
            wb = ReportWorkBookHandler(
                g,
                this_report.components,
                this_report.excel_template_location,
            )
            final_list.append(wb)
        return final_list
