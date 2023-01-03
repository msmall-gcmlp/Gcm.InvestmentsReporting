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
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from .....core.components.report_table import ReportTable
from ....report_names import ReportNames
from .construct_data import BbaReport

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=AggregatedPortolioFundAttributeReport&frequency=Monthly&save=True&EntityDomainTypes=Vertical&EntityNames=["ARS"]


class AggregatedPortolioFundAttributeReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(ReportNames.AggregatedPortolioFundAttributeReport, report_meta)

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
        file = None
        # TODO: Confirm with David
        current_domain: EntityDomainTypes = self.report_meta.entity_domain
        if current_domain == EntityDomainTypes.Vertical:
            file = "PFUND_Attributes_Template.xlsx"
        if file is not None:
            return AzureDataLakeDao.BlobFileStructure(
                zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
                sources="investmentsreporting",
                entity="exceltemplates",
                path=[file],
            )
        raise NotImplementedError()

    def assign_components(self):
        with Scenario(
            runner=Scenario.get_attribute("dao"),
        ).context():
            report = BbaReport()
            d = report.generate_pfund_attributes()
            final = []
            for k, v in d.items():
                final.append(ReportTable(k, v))
            return final
