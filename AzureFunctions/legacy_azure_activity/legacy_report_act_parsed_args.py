from ..legacy_azure_orchestrator.legacy_report_orch_parsed_args import (
    LegacyReportingOrchParsedArgs,
)
from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from enum import auto


class LegacyActivities(ExtendedEnum):
    BaselReportActivity = auto()
    BbaReportActivity = auto()
    EofLiquidityStressReportActivity = auto()
    EofRbaReportActivity = auto()
    HkmaMarketPerformanceReportActivity = auto()
    MarketPerformanceReportActivity = auto()
    PerformanceScreenerReportActivity = auto()


class LegacyReportingActivityParsedArgs(LegacyReportingOrchParsedArgs):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_dict(cls, d: dict):
        pargs = super().from_dict(d)
        return pargs
