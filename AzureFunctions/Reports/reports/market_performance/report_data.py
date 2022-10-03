from gcm.inv.dataprovider.factor import Factor
from gcm.inv.reporting.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.timeseries.transformer.level_change import (
    LevelChange,
)
from gcm.inv.utils.date import DatePeriod


class MarketPerformanceQualityReportData(ReportingRunnerBase):
    def __init__(self, runner, start_date, as_of_date, ticker_map):
        super().__init__(runner=runner)
        self._start_date = start_date
        self._as_of_date = as_of_date
        self._analytics = Analytics()
        self._ticker_map = ticker_map

        factors = Factor(tickers=ticker_map.Ticker.values.tolist())
        self._factors = factors

    def get_market_performance_quality_report_inputs(self):
        # pre-filtering to EMMs to avoid performance issues. refactor later to occur behind the scenes in data provider
        # add relative patch
        # extract factor returns

        market_factor_returns = self._factors.get_returns(
            start_date=self._start_date,
            end_date=self._as_of_date,
            fill_na=True,
        )
        # extract factor prices
        market_factor_data = self._factors.get_dimensions(date_period=DatePeriod(start_date=self._start_date, end_date=self._as_of_date))

        price = market_factor_data.pivot_table(index="Date", columns="Ticker", values="PxLast")
        # Calculate level change for yields
        level_change = LevelChange().transform(price)

        return (market_factor_returns, price, level_change)

    def run(self, **kwargs):
        return self.get_market_performance_quality_report_inputs()
