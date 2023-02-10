import pandas as pd
import datetime as dt

from _legacy.Reports.reports.eof_crowding.eof_crowding_report import generate_eof_crowding_report_data
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


class RunEofCrowdingReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date

    def generate_report(self):
        longs, shorts = generate_eof_crowding_report_data(runner=self._runner, date=self._as_of_date)
        input_data = {
            'as_of_date': pd.DataFrame({'date': [self._as_of_date]}),
            'crowded_longs': longs,
            'crowded_shorts': shorts
        }

        date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(as_of_date=date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="EOF_HF_Technicals_Exposure_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name="Equity Opps Fund Ltd",
                entity_display_name="EOF",
                entity_ids=[19163],
                entity_source=DaoSource.PubDwh,
                report_name="EOF HF Technicals Exposure",
                report_type=ReportType.Risk,
                report_vertical=ReportVertical.ARS,
                report_frequency="Daily",
                aggregate_intervals=AggregateInterval.Daily,
                # print_areas=print_areas,
                # output_dir="cleansed/investmentsreporting/printedexcels/",
                # report_output_source=DaoSource.DataLake,
            )

    def run(self, **kwargs):
        self.generate_report()
        return "Complete"


if __name__ == "__main__":
    dao_runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.DataLake.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                    DaoSource.PubDwh.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                    DaoSource.InvestmentsDwh.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                    DaoSource.DataLake_Blob.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                    DaoSource.ReportingStorage.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                }
            })

    date = dt.date(2023, 1, 31)
    with Scenario(as_of_date=date).context():
        RunEofCrowdingReport(runner=dao_runner, as_of_date=date).execute()
