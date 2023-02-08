from gcm.inv.utils.azure.legacy_conversion.legacy_activity import (
    LegacyActivity,
)
from .legacy_report_act_parsed_args import (
    LegacyReportingActivityParsedArgs,
    LegacyActivities,
)
from .activities.BaselReportActivity import main as basel_report_main
from .activities.BbaReportActivity import main as bba_report_main
from .activities.EofLiquidityStressReportActivity import (
    main as eof_liq_report_main,
)
from .activities.EofRbaReportActivity import (
    main as eof_rba_report_main,
)
from .activities.HkmaMarketPerformanceReportActivity import (
    main as hkma_report_main,
)
from .activities.MarketPerformanceReportActivity import (
    main as market_report_main,
)
from .activities.PerformanceScreenerReportActivity import (
    main as perf_screener_main,
)
from .activities.SingleNameEquityActivity import (
    main as singlename_equityexposure_main,
)
from .activities.XPFundPqReportActivity import (
    main as x_pfund_pq_report_main,
)


class LegacyReportConstructorActivity(LegacyActivity):
    def __init__(self):
        super().__init__()

    @property
    def activity_map(self):
        return {
            LegacyActivities.BaselReportActivity: basel_report_main,
            LegacyActivities.BbaReportActivity: bba_report_main,
            LegacyActivities.EofLiquidityStressReportActivity: eof_liq_report_main,
            LegacyActivities.EofRbaReportActivity: eof_rba_report_main,
            LegacyActivities.HkmaMarketPerformanceReportActivity: hkma_report_main,
            LegacyActivities.MarketPerformanceReportActivity: market_report_main,
            LegacyActivities.PerformanceScreenerReportActivity: perf_screener_main,
            LegacyActivities.SingleNameEquityActivity: singlename_equityexposure_main,
            LegacyActivities.XPFundPqReportActivity: x_pfund_pq_report_main,
        }

    @property
    def parg_type(self):
        return LegacyReportingActivityParsedArgs


def main(context):
    return LegacyReportConstructorActivity().execute(context=context)
