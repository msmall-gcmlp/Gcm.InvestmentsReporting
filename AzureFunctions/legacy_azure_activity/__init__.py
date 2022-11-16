from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
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
import json
from typing import Tuple


class LegacyReportConstructorActivity(BaseActivity):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return LegacyReportingActivityParsedArgs

    def get_activity_info(self) -> Tuple[dict, LegacyActivities, dict]:
        data = self._d["data"]
        activity_name = LegacyActivities[self._d["_legacy_activity_name"]]
        activity_params = json.loads(self._d["_legacy_activity_params"])
        return (data, activity_name, activity_params)

    def activity(self, **kwargs):
        assert self.pargs is not None
        d = None
        [data, activity_name, activity_params] = self.get_activity_info()
        assert data is not None
        if activity_name == LegacyActivities.BaselReportActivity:
            d = basel_report_main(activity_params)
        if activity_name == LegacyActivities.BbaReportActivity:
            d = bba_report_main(activity_params)
        if (
            activity_name
            == LegacyActivities.EofLiquidityStressReportActivity
        ):
            d = eof_liq_report_main(activity_params)
        if activity_name == LegacyActivities.EofRbaReportActivity:
            d = eof_rba_report_main(activity_params)
        if (
            activity_name
            == LegacyActivities.HkmaMarketPerformanceReportActivity
        ):
            d = hkma_report_main(activity_params)
        if (
            activity_name
            == LegacyActivities.MarketPerformanceReportActivity
        ):
            d = market_report_main(activity_params)
        if (
            activity_name
            == LegacyActivities.PerformanceScreenerReportActivity
        ):
            d = perf_screener_main(activity_params)

        return d


def main(context):
    return LegacyReportConstructorActivity().execute(context=context)
