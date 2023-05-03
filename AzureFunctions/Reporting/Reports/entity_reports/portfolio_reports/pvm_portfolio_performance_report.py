from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AzureDataLakeDao,
)
from ..utils.PvmTrackRecord.base_pvm_tr_report import (
    BasePvmTrackRecordReport,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
    Standards as EntityDomainStandards,
)
from ....core.components.report_table import ReportTable
import pandas as pd
from ...report_names import ReportNames


class PvmPortfolioPerformanceTabs(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=[""],
        )
