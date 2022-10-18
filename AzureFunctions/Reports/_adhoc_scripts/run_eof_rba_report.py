import datetime as dt
from gcm.inv.scenario import Scenario
from Reports.reports.eof_rba.eof_rba_report import EofReturnBasedAttributionReport
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.inv.quantlib.enum_source import PeriodicROR
from gcm.Dao.DaoSources import DaoSource


class RunEofReturnBasedAttributionReport:
    def __init__(self, as_of_date):
        self._runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.InvestmentsDwh.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    DaoSource.DataLake.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                }
            },
        )
        self._as_of_date = as_of_date

    def generate_report(self, periodicity):
        with Scenario(runner=self._runner, as_of_date=self._as_of_date, periodicity=periodicity).context():
            eof_rba = EofReturnBasedAttributionReport()
            return eof_rba.execute()


if __name__ == "__main__":
    report_runner = RunEofReturnBasedAttributionReport(as_of_date=dt.date(2022, 8, 31))
    report_runner.generate_report(periodicity=PeriodicROR.ITD)
    report_runner.generate_report(periodicity=PeriodicROR.YTD)
