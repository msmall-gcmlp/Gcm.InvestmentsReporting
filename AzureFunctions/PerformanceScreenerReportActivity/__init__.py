from datetime import datetime
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner
from Reports.reports.performance_screener_report import PerformanceScreenerReport


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    asofdate = params["asofdate"]
    peer_group = params["peer_group"]
    as_of_date = datetime.strptime(asofdate, "%Y-%m-%d").date()
    runner = DaoRunner()

    if run == "PerformanceScreenerReport":
        with Scenario(runner=runner, as_of_date=as_of_date).context():
            screener = PerformanceScreenerReport(peer_group=peer_group)
            return screener.execute()
