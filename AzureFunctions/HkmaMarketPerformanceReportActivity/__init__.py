from datetime import datetime
from _legacy.Reports.reports.market_performance.hkma_market_performance_report import HkmaMarketPerformanceReport
from gcm.Dao.DaoRunner import DaoRunner
from gcm.inv.scenario import Scenario


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    runner = DaoRunner()

    if run == "HkmaMarketPerformanceReport":
        with Scenario(runner=runner, as_of_date=as_of_date).context():
            return HkmaMarketPerformanceReport().execute()
