import datetime as dt
import json
import pandas as pd
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.performance_quality.xpfund_highlow_data_analysis import _xpfund_data_to_highlow_df
from functools import cached_property
from _legacy.Reports.reports.performance_quality.helper import PerformanceQualityHelper
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)


class XPfundHighLowPQScreen(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, portfolio_acronym=None):
        #super().__init__(runner=Scenario.get_attribute("runner"))
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._portfolio_acronym = portfolio_acronym

    def _download_inputs(self, runner, dl_location, file_path) -> dict:
        try:
            read_params = AzureDataLakeDao.create_get_data_params(
                dl_location,
                file_path,
                retry=False,
            )
            file = runner.execute(
                params=read_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.get_data(read_params),
            )
            inputs = json.loads(file.content)
        except:
            inputs = None
        return inputs

    @cached_property
    def _xpfund_pq_report(self):       
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        inputs = PerformanceQualityHelper().download_inputs(
            location="raw/investmentsreporting/summarydata/xpfund_performance_quality", 
            file_path ="_firm_x_portfolio_fund" + as_of_date + ".json"
        )
        return inputs

    def _get_xpfund_pq_report(self, runner, as_of_date):
        inputs = self._xpfund_pq_report
        xpfund_inputs = pd.read_json(inputs['report_data'], orient='index')

        return xpfund_inputs

    def generate_xpfund_high_pq_screen_data(self):
        with Scenario(dao=self._runner, as_of_date=self._as_of_date).context():
            date_q_minus_1 = pd.to_datetime(self._as_of_date - pd.tseries.offsets.QuarterEnd(1)).date()
            firm_xpfund_report_data = self._get_xpfund_pq_report(runner=self._runner, as_of_date=date_q_minus_1)
            firm_xpfund_highlow_df = firm_xpfund_report_data.copy()
            firm_xpfund_highlow_df = _xpfund_data_to_highlow_df(firm_xpfund_highlow_df, self._as_of_date, portfolio_acronym=self._portfolio_acronym)

        if (self._portfolio_acronym is None):
            report_name = "ARS Performance Quality - Firmwide High Low Performance Screen"
            high_rep_data = {
                'as_of_date1': pd.DataFrame({'date': [self._as_of_date]}),
                'as_of_date2': pd.DataFrame({'date': [self._as_of_date]}),
                "high_perf_summary": firm_xpfund_highlow_df[0],
                "high_perf_data": firm_xpfund_highlow_df[1],
                "low_perf_summary": firm_xpfund_highlow_df[2],
                "low_perf_data": firm_xpfund_highlow_df[3],
            }
            with Scenario(as_of_date=self._as_of_date).context():
                InvestmentsReportRunner().execute(
                    data=high_rep_data,
                    template="highlow_pq_1.xlsx",
                    save=True,
                    runner=self._runner,
                    entity_type=ReportingEntityTypes.cross_entity,
                    entity_name='FIRM',
                    entity_display_name='FIRM',
                    report_name=report_name,
                    report_type=ReportType.Performance,
                    report_vertical=ReportVertical.ARS,
                    report_frequency="Monthly",
                    aggregate_intervals=AggregateInterval.MTD,
                )

        else:
            report_name = "ARS Performance Quality - Portfolio High Low Performance Screen"
            high_rep_data = {
                'as_of_date1': pd.DataFrame({'date': [self._as_of_date]}),
                'as_of_date2': pd.DataFrame({'date': [self._as_of_date]}),
                'portfolio_name1': pd.DataFrame({'acronym': [self._portfolio_acronym]}),
                'portfolio_name2': pd.DataFrame({'acronym': [self._portfolio_acronym]}),
                "high_perf_summary": firm_xpfund_highlow_df[0],
                "high_perf_data": firm_xpfund_highlow_df[1],
                "low_perf_summary": firm_xpfund_highlow_df[2],
                "low_perf_data": firm_xpfund_highlow_df[3],
            }
            with Scenario(as_of_date=self._as_of_date).context():
                InvestmentsReportRunner().execute(
                    data=high_rep_data,
                    template="portfolio_highlow_pq.xlsx",
                    save=True,
                    runner=self._runner,
                    entity_type=ReportingEntityTypes.cross_entity,
                    entity_name='Portfolio',
                    entity_display_name=self._portfolio_acronym,
                    report_name=report_name,
                    report_type=ReportType.Performance,
                    report_vertical=ReportVertical.ARS,
                    report_frequency="Monthly",
                    aggregate_intervals=AggregateInterval.MTD,
                )

        return firm_xpfund_highlow_df

    def run(self, **kwargs):
        try:
            self.generate_xpfund_high_pq_screen_data()
            return "Complete"
        except Exception as e:
            raise RuntimeError("Failed") from e


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
            }
        },
    )
    date = dt.date(2023, 6, 30)
    #portfolio_acronym = 'GIP'
    inv_group_ids = None
    with Scenario(as_of_date=date).context():
        report = XPfundHighLowPQScreen(runner=dao_runner, as_of_date=date, portfolio_acronym='GIP')
        #report = XPfundHighLowPQScreen(runner=dao_runner, as_of_date=date)
        report.execute()
