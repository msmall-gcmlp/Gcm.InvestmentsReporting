from enum import Enum, auto


class LegacyOrchestrations(Enum):
    BaselReportOrchestrator = auto()
    BbaReportOrchestrator = auto()
    EofLiquidityStressReportOrchestrator = auto()
    EofRbaReportOrchestrator = auto()
    HkmaMarketPerformanceReportOrchestrator = auto()
  
    PerformanceScreenerReportOrchestrator = auto()
    # TODO: refactor below to be scenario compliant
    # ReportCopyOrchestrator = auto()

    # Excluding below because too complicated to port over
    # TODO: refactor appropriately to make Scenario compliant
    # PerformanceQualityReportOrchestrator = auto()
