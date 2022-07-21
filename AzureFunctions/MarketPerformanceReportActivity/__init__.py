from pandas.tseries.offsets import BDay
from datetime import datetime
from gcm.inv.reporting.reports.market_performance.report_data import (
    MarketPerformanceQualityReportData,
)
from gcm.inv.reporting.reports.market_performance.market_performance_report import (
    MarketPerformanceReport,
)
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Scenario.scenario import Scenario


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    asofdate = params["asofdate"]
    as_of_date = datetime.strptime(asofdate, "%Y-%m-%d").date()
    start_date = as_of_date - BDay(253)
    runner = DaoRunner()

    if run == "RunMarketPerformanceQualityReportData":
        with Scenario(runner=runner, as_of_date=as_of_date).context():
            input_data = MarketPerformanceQualityReportData(
                start_date=start_date,
                runner=runner,
                as_of_date=as_of_date,
            ).execute()
        runner2 = DaoRunner()
        MarketPerformance = MarketPerformanceReport(
            runner=runner2,
            as_of_date=as_of_date,
            interval="MTD",
            factor_daily_returns=input_data[0],
            prices=input_data[1],
            price_change=input_data[2],
            ticker_mapping=input_data[3],
        )

        return MarketPerformance.execute()
