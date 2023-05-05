from .market_reports.market_performance_report import (
    MarketPerformanceReport,
)
from .entity_reports.portfolio_reports.sample_ars_portfolio_report import (
    SampleArsPortfolioReport,
)
from .entity_reports.xentity_reports.ars_bba_report import (
    BrinsonAttributionReport,
)
from .entity_reports.vertical_reports.ars_pfund_attributes.aggregated_pfund_attribute_report import (
    AggregatedPortolioFundAttributeReport,
)
from .entity_reports.investment_manager_reports.pvm_manager_trackrecord_report import (
    PvmManagerTrackRecordReport,
)
from .entity_reports.investment_reports.pvm_investment_trackrecord_report import (
    PvmInvestmentTrackRecordReport,
)
from .entity_reports.xentity_reports.pvm_portfolio_performance_report import (
    PvmPerformanceBreakoutReport,
)
from .report_names import ReportNames
from ..core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
    Frequency,
    Standards,
)
from gcm.inv.utils.date.Frequency import FrequencyType
from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.business_calendar import BusinessCalendar
from typing import List
import pandas as pd


# poor-mans class reflection
def get_report_class_by_name(name: ReportNames):
    if name == ReportNames.MarketPerformanceReport:
        return MarketPerformanceReport
    if name == ReportNames.Sample_Ars_Portfolio_Report:
        return SampleArsPortfolioReport
    if name == ReportNames.BrinsonAttributionReport:
        return BrinsonAttributionReport
    if name == ReportNames.AggregatedPortolioFundAttributeReport:
        return AggregatedPortolioFundAttributeReport
    if name == ReportNames.PvmManagerTrackRecordReport:
        return PvmManagerTrackRecordReport
    if name == ReportNames.PvmInvestmentTrackRecordReport:
        return PvmInvestmentTrackRecordReport
    if name == ReportNames.PvmPerformanceBreakoutReport:
        return PvmPerformanceBreakoutReport
    else:
        raise NotImplementedError()


def validate_meta(
    report_structure: ReportStructure,
    report_meta: ReportMeta,
    strict: bool = True,
):
    available_metas: AvailableMetas = report_structure.available_metas()
    assert available_metas is not None
    assert report_meta.interval in available_metas.aggregate_intervals
    as_of_date = Scenario.get_attribute("as_of_date")
    frequency_type: FrequencyType = report_meta.frequency.type
    if strict:
        frequencies: List[Frequency] = available_metas.frequencies
        # check if we're running on the right date?
        final_freq: Frequency = None
        for f in frequencies:
            if BusinessCalendar().is_business_day(as_of_date, f.calendar):
                final_freq = Frequency(frequency_type, f.calendar)
                break
        assert final_freq is not None
    entity_info: pd.DataFrame = report_meta.entity_info
    domain = report_meta.entity_domain
    if available_metas.entity_groups is not None:
        assert (
            domain is not None and domain in available_metas.entity_groups
        )
        if strict:
            assert (
                len(
                    list(
                        entity_info[Standards.EntityName].dropna().unique()
                    )
                )
                == 1
            )
