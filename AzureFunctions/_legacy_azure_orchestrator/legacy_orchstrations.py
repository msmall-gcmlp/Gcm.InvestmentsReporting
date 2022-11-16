from enum import Enum, auto

class LegacyOrchestrations(Enum):
    BaselReportOrchestrator = auto()
    BbaReportOrchestrator = auto()
    EofLiquidityStressReportOrchestrator = auto()
    EofRbaReportOrchestrator = auto()
    HkmaMarketPerformanceReportOrchestrator = auto()
    PerformanceQualityReportOrchestrator = auto()
    PerformanceScreenerReportOrchestrator = auto()
    ReportCopyOrchestrator = auto()