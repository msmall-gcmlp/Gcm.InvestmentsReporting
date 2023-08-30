from gcm.inv.utils.misc.extended_enum import ExtendedEnum
from enum import auto


class ReportNames(ExtendedEnum):
    PerformanceQualityReport = auto()
    MarketPerformanceReport = auto()
    BrinsonAttributionReport = auto()
    MarketDataDashboard_Credit = auto()
    PeerRankingReport = auto()
    ESG_ScoreCard = auto()
    Sample_Ars_Portfolio_Report = auto()
    AggregatedPortolioFundAttributeReport = auto()
    BasePvmTrackRecordReport = auto()

    PvmManagerTrackRecordReport = auto()
    PvmInvestmentTrackRecordReport = auto()
    PvmManagerTrackRecordReportAggregation = auto()
    PvmTrAttributionReport = auto()

    PvmPerformanceBreakoutReport = auto()
    PE_Portfolio_Performance_x_Vintage_Realization_Status = (
        "PE Portfolio Performance x Vintage & Realization Status"
    )
    PE_Portfolio_Performance_x_Investment_Manager = (
        "PE Portfolio Performance x Investment Manager"
    )
    PE_Portfolio_Performance_x_Sector = "PE Portfolio Performance x Sector"
    PE_Portfolio_Performance_x_Region = "PE Portfolio Performance x Region"
