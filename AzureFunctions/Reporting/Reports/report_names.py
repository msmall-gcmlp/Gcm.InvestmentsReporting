from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from enum import auto


class ReportNames(ExtendedEnum):
    PerformanceQualityReport = auto()
    MarketPerformanceReport = auto()
    BrinsonAttributionReport = auto()
    MarketDataDashboard_Credit = auto()
    PeerRankingReport = auto()
