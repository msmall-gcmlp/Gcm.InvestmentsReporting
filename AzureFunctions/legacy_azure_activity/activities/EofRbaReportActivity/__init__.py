from datetime import datetime
from gcm.inv.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner
from gcm.inv.quantlib.enum_source import PeriodicROR
from _legacy.Reports.reports.eof_rba.eof_rba_report import (
    EofReturnBasedAttributionReport,
)


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    runner = DaoRunner()

    if run == "EofRbaReport":
        with Scenario(runner=runner, as_of_date=as_of_date, periodicity=PeriodicROR.ITD).context():
            EofReturnBasedAttributionReport().execute()

        with Scenario(runner=runner, as_of_date=as_of_date, periodicity=PeriodicROR.YTD).context():
            EofReturnBasedAttributionReport().execute()

        with Scenario(runner=runner, as_of_date=as_of_date, periodicity='TTM').context():
            EofReturnBasedAttributionReport().execute()

        return 'Complete'
