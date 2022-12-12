from .market_reports.market_performance_report import (
    MarketPerformanceReport,
)
from .esg_reports.esg_score_card import ESG_ScoreCard
from .report_names import ReportNames


# poor-mans class reflection
def get_report_class_by_name(name: ReportNames):
    if name == ReportNames.MarketPerformanceReport:
        return MarketPerformanceReport
    if name == ReportNames.ESG_ScoreCard:
        return ESG_ScoreCard
    else:
        raise NotImplementedError()
