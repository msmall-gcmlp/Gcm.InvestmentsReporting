from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AzureDataLakeDao,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
    Standards as EntityStandardNames,
    AvailableMetas,
    ReportType,
    Frequency,
    FrequencyType,
    AggregateInterval,
    ReportConsumer,
)
from ...report_names import ReportNames


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
                Frequency(FrequencyType.Monthly),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.PM],
                vertical=ReportConsumer.Vertical.ARS,
            ),
            entity_groups=[
                EntityDomainTypes.Portfolio,
                EntityDomainTypes.InvestmentManager,
            ],
        )

    def assign_components(self):
        assert self.report_meta.entity_domain == EntityDomainTypes.Vertical
        names = [
            x
            for x in self.report_meta.entity_info[
                EntityStandardNames.EntityName
            ]
            .drop_duplicates()
            .to_list()
        ]
        assert all([x == "ARS" for x in names]) and len(names) == 1
