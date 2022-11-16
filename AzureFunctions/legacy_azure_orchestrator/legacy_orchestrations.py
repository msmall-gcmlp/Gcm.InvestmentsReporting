from enum import auto
from gcm.inv.utils.misc.extended_enum import ExtendedEnum


class LegacyOrchestrations(ExtendedEnum):
    BaselReportOrchestrator = auto()
    BbaReportOrchestrator = auto()
    EofLiquidityStressReportOrchestrator = auto()
    EofRbaReportOrchestrator = auto()
    MarketPerformanceReportOrchestrator = auto()
    HkmaMarketPerformanceReportOrchestrator = auto()
    PerformanceScreenerReportOrchestrator = auto()

    # TODO: refactor below to be scenario compliant
    # ReportCopyOrchestrator = auto()

    # Excluding below because too complicated to port over
    # TODO: refactor appropriately to make Scenario compliant
    # PerformanceQualityReportOrchestrator = auto()
