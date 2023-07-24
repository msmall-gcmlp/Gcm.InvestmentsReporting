from enum import Enum
from .....core.report_structure import ReportMeta
from . import BasePvmTrackRecordReport
from .....core.report_structure import (
    EntityDomainTypes,
)
from ....report_names import ReportNames


class PvmInvestmentTrackRecordReport(BasePvmTrackRecordReport):
    def __init__(self, report_name: Enum, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmInvestmentTrackRecordReport, report_meta
        )

    def level(cls):
        return EntityDomainTypes.Investment
