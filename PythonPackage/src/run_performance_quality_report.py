import datetime as dt

from pandas._libs.tslibs.offsets import relativedelta

from gcm.inv.reporting.reports.aggregate_performance_quality_report import AggregatePerformanceQualityReport
from gcm.inv.reporting.reports.performance_quality_report_data import PerformanceQualityReportData
from gcm.inv.reporting.reports.performance_quality_report import PerformanceQualityReport
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource

from gcm.inv.reporting.reports.report_binder import ReportBinder


class RunPerformanceQualityReports:

    def __init__(self, as_of_date):
        self._runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.PubDwh.name: {
                        "Environment": "uat",
                        "Subscription": "nonprd",
                    },
                }
            })
        self._as_of_date = as_of_date
        self._params = {'status': 'EMM', 'vertical': 'ARS', 'entity': 'PFUND'}
        self._perf_quality_data = PerformanceQualityReportData(
            runner=self._runner,
            start_date=as_of_date - relativedelta(years=10),
            end_date=as_of_date,
            as_of_date=as_of_date,
            params=self._params)

    def generate_report_data(self):
        return self._perf_quality_data.execute()

    def generate_fund_reports(self, fund_names):
        for fund in fund_names:
            params = self._params.copy()
            params['fund_name'] = fund
            perf_quality_report = PerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date,
                                                           params=params)
            perf_quality_report.execute()

    def combine_by_portfolio(self, portfolio_acronyms):
        agg_perf_quality = ReportBinder(runner=self._runner, as_of_date=self._as_of_date,
                                        portfolio_acronyms=portfolio_acronyms)
        agg_perf_quality.execute()

    def agg_perf_quality_by_portfolio(self, portfolio_acronyms):
        for acronym in portfolio_acronyms:
            params = self._params.copy()
            agg_perf_quality = AggregatePerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date,
                                                                 acronym=acronym, params=params)
            agg_perf_quality.execute()


if __name__ == "__main__":
    report_runner = RunPerformanceQualityReports(as_of_date=dt.date(2022, 3, 31))
    #fund_names = report_runner.generate_report_data()
    #report_runner.generate_fund_reports(fund_names=['Citadel', 'Skye', 'D1 Capital'])
    aggregate_return_summary = report_runner.agg_perf_quality_by_portfolio(portfolio_acronyms=['GIP', 'IFC'])
    #report_runner.combine_by_portfolio(portfolio_acronyms=['GIP', 'IFC'])

    # High Priority
    # TODO add folder structure to data lake dumps/pass in file paths
    # TODO remove duplicate funds (same group) from FactorAnalysis_New
    # TODO add strategy peer group

    # Next up
    # TODO add to Data pipelines imports of RBA, PBA, and Abs Benchmark Returns
    # TODO populate HYG
    # TODO icelandic method
    # TODO review excess aggregation assumption w/ amy (i.e. Fund returns without Abs Bmrk returns)
    # TODO incorporate macro model
    # TODO run peer group stats only once
    # TODO update report tagging/write to reporting hub
    # TODO check Aspex RBA/compounding
    # TODO Add asofdate to params
    # TODO Document report and data provider (document default waterfall logic)
