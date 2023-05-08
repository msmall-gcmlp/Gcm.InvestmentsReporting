from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
    AvailableMetas,
    ReportType,
    Frequency,
    FrequencyType,
    AggregateInterval,
    ReportConsumer,
    Calendar,
)
from ....core.components.report_table import ReportTable
from typing import List
from ...report_names import ReportNames
from ..utils.pvm_performance_utils.pvm_performance_helper import (
    PvmPerformanceHelper,
)
from gcm.inv.scenario import Scenario
import datetime as dt
from ....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)


# Run all PEREI entities:
# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-12-31&ReportName=PvmPerformanceBreakoutReport&test=True&frequency=Quarterly&save=True&aggregate_interval=Multi&EntityDomainTypes=Portfolio&GetBy=PEREI_Entities
# Run a single entity:
# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-12-31&ReportName=PvmPerformanceBreakoutReport&test=True&frequency=Quarterly&save=True&aggregate_interval=Multi&EntityDomainTypes=Portfolio&EntityNames=[%22The%20Consolidated%20Edison%20Pension%20Plan%20Master%20Trust%20-%20GCM%20PE%20Account%22]
class PvmPerformanceBreakoutReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmPerformanceBreakoutReport, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["TWROR_Template_threey.xlsx"],
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Quarterly, Calendar.AllDays),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.FIRM],
                vertical=ReportConsumer.Vertical.PEREI,
            ),
            entity_groups=[
                EntityDomainTypes.Portfolio,
                EntityDomainTypes.InvestmentManager,
            ],
        )

    def assign_components(self) -> List[ReportWorkBookHandler]:
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        domain = self.report_meta.entity_domain
        entity_info = self.report_meta.entity_info
        p = PvmPerformanceHelper(domain, entity_info=entity_info)
        final_data: dict = p.generate_components_for_this_entity(
            as_of_date
        )
        tables: List[ReportTable] = []
        for k, v in final_data.items():
            this_table = ReportTable(k, v)
            tables.append(this_table)
        workbook = ReportWorkBookHandler(
            f"{self.report_meta.entity_domain.name}_Perf",
            tables,
            self.excel_template_location,
        )
        return [workbook]
