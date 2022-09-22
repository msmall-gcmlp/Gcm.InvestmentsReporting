from datetime import datetime
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner
from gcm.inv.quantlib.enum_source import PeriodicROR
from Reports.reports.eof_rba_report import EofReturnBasedAttributionReport


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    asofdate = params["asofdate"]
    periodicity = params["periodicity"]
    as_of_date = datetime.strptime(asofdate, "%Y-%m-%d").date()
    runner = DaoRunner()

    if periodicity == "ITD":
        periodicity = PeriodicROR.ITD
    elif periodicity == "YTD":
        periodicity = PeriodicROR.YTD

    if run == "EofRbaReport":
        with Scenario(runner=runner, as_of_date=as_of_date, periodicity=periodicity).context():
            eof_rba = EofReturnBasedAttributionReport()
            return eof_rba.execute()
