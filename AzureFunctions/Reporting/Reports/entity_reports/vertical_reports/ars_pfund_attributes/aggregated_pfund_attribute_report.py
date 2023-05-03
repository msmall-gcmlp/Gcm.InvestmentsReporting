from .....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from .....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
    Scenario,
    EntityStandardNames,
)
from .....core.components.report_table import ReportTable
from ......_legacy.Reports.reports.brinson_based_attribution.bba_report import (
    BbaReport,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from ....report_names import ReportNames

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=AggregatedPortolioFundAttributeReport&frequency=Monthly&save=True


class AggregatedPortolioFundAttributeReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.AggregatedPortolioFundAttributeReport, report_meta
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Monthly),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.PM],
                vertical=ReportConsumer.Vertical.ARS,
            ),
            entity_groups=[
                EntityDomainTypes.Vertical,
            ],
        )

    @property
    def excel_template_location(self):
        file = "PFUND_Attributes_Template.xlsx"
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=[file],
        )

    def report_name_metadata(self):
        return "ARS Portfolio Fund Attributes"

    def assign_components(self):
        with Scenario(
            runner=Scenario.get_attribute("dao"),
        ).context():
            assert (
                self.report_meta.entity_domain
                == EntityDomainTypes.Vertical
            )
            names = [
                x
                for x in self.report_meta.entity_info[
                    EntityStandardNames.EntityName
                ]
                .drop_duplicates()
                .to_list()
            ]
            assert all([x == "ARS" for x in names] and len(names) == 1)
            report = BbaReport()
            this_passed_vertical_name = names[0]
            d: dict = report.generate_pfund_attributes(
                this_passed_vertical_name
            )
            final = []
            for k, v in d.items():
                final.append(ReportTable(k, v))
            return final
