import pandas as pd
import datetime as dt

from _legacy.Reports.reports.performance_quality.xpfund_pq_report import generate_xpfund_pq_report_data
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs, DaoSource
from gcm.inv.scenario import Scenario


class RunXPFundPqReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date

    def generate_report(self):
        report_data = generate_xpfund_pq_report_data(runner=dao_runner, date=self._as_of_date)
        input_data = {
            'as_of_date': pd.DataFrame({'date': [self._as_of_date]}),
            'report_data': report_data
        }

        print_areas = {'XPFUND_Performance_Quality': 'FL1:FU3'}

        InvestmentsReportRunner().execute(
            data=input_data,
            template="XPFUND_PerformanceQuality_Template.xlsx",
            save=True,
            runner=self._runner,
            entity_type=ReportingEntityTypes.cross_entity,
            entity_name='FIRM',
            entity_display_name='FIRM',
            report_name="ARS Performance Quality - Firm x Portfolio Fund",
            report_type=ReportType.Performance,
            report_vertical=ReportVertical.ARS,
            report_frequency="Monthly",
            aggregate_intervals=AggregateInterval.MTD,
            print_areas=print_areas,
            # output_dir="cleansed/investmentsreporting/printedexcels/",
            # report_output_source=DaoSource.DataLake,
        )

    def run(self, **kwargs):
        try:
            self.generate_report()
            return "Complete"
        except Exception as e:
            raise RuntimeError("Failed") from e


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
            }
        },
    )

    date = dt.date(2022, 12, 31)
    with Scenario(as_of_date=date).context():
        RunXPFundPqReport(runner=dao_runner, as_of_date=date).execute()
