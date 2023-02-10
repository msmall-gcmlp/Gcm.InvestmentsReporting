from ..legacy_azure_orchestrator.legacy_report_orch_parsed_args import (
    LegacyReportingOrchParsedArgs,
    LegacyParsedArgs,
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
    SingleNameEquityActivity = auto()
    XPFundPqReportActivity = auto()
    EofCrowdingReportActivity = auto()


class LegacyReportingActivityParsedArgs(LegacyReportingOrchParsedArgs):
    def __init__(self):
        super().__init__()

    Activity_EnumType = LegacyActivities
    PargType = LegacyParsedArgs.LegacyPargType.Legacy_Activity
