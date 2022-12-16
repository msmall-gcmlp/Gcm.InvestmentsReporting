from .market_reports.market_performance_report import (
    MarketPerformanceReport,
)
from .misc.esg_reports.esg_score_card import ESG_ScoreCard
from .entity_reports.portfolio_reports.sample_ars_portfolio_report import (
    SampleArsPortfolioReport,
)
from .entity_reports.xentity_reports.ars_bba_report import (
    BrinsonAttributionReport,
)
from .entity_reports.vertical_reports.ars_pfund_attributes.aggregated_pfund_attribute_report import (
    AggregatedPortolioFundAttributeReport,
)
from .report_names import ReportNames


# poor-mans class reflection
def get_report_class_by_name(name: ReportNames):
    if name == ReportNames.MarketPerformanceReport:
        return MarketPerformanceReport
    if name == ReportNames.ESG_ScoreCard:
        return ESG_ScoreCard
    if name == ReportNames.Sample_Ars_Portfolio_Report:
        return SampleArsPortfolioReport
    if name == ReportNames.BrinsonAttributionReport:
        return BrinsonAttributionReport
    if name == ReportNames.AggregatedPortolioFundAttributeReport:
        return AggregatedPortolioFundAttributeReport
    else:
        raise NotImplementedError()
