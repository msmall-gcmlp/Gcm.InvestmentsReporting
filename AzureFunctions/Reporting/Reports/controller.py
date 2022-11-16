from .market_reports.market_performance_report import (
    MarketPerformanceReport,
)
from .report_names import ReportNames


# poor-mans class reflection
def get_report_class_by_name(name: ReportNames):
    if name == ReportNames.MarketPerformanceReport:
        return MarketPerformanceReport
    else:
        raise NotImplementedError()
