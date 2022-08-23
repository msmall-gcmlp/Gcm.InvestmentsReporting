import datetime as dt
from gcm.Scenario.scenario import Scenario
from gcm.inv.reporting.reports.eof_rba_report import EofReturnBasedAttributionReport
from gcm.Dao.DaoRunner import DaoRunner
from gcm.inv.quantlib.enum_source import PeriodicROR


class RunEofReturnBasedAttributionReport:

    def __init__(self, as_of_date):
        self._runner = DaoRunner()
        self._as_of_date = as_of_date
        self._params = {'status': 'EMM', 'vertical': 'ARS', 'entity': 'PFUND'}

    def generate_report(self, periodicity):
        with Scenario(runner=self._runner, as_of_date=self._as_of_date, periodicity=periodicity).context():
            eof_rba = EofReturnBasedAttributionReport()
            return eof_rba.execute()


if __name__ == "__main__":
    report_runner = RunEofReturnBasedAttributionReport(as_of_date=dt.date(2022, 4, 30))
    funds_and_peers = report_runner.generate_report(periodicity=PeriodicROR.YTD)
