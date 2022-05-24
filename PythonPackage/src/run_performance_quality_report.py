import datetime as dt
import json
from pandas._libs.tslibs.offsets import relativedelta
from gcm.inv.reporting.reports.aggregate_performance_quality_report import AggregatePerformanceQualityReport
from gcm.inv.reporting.reports.performance_quality_peer_summary_report import PerformanceQualityPeerSummaryReport
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

    def generate_report_data(self, investment_ids):
        perf_quality_data = PerformanceQualityReportData(
            runner=self._runner,
            start_date=self._as_of_date - relativedelta(years=10),
            end_date=self._as_of_date,
            as_of_date=self._as_of_date,
            investment_ids=investment_ids)
        return perf_quality_data.execute()

    def generate_peer_summaries(self, peer_groups):
        for peer_group in peer_groups:
            perf_quality_data = PerformanceQualityPeerSummaryReport(
                runner=self._runner,
                as_of_date=self._as_of_date,
                peer_group=peer_group)
            perf_quality_data.execute()

    def generate_fund_reports(self, fund_names):
        for fund in fund_names:
            params = self._params.copy()
            params['fund_name'] = fund
            perf_quality_report = PerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date,
                                                           fund_name=fund)
            perf_quality_report.execute()

    def combine_by_portfolio(self, portfolio_acronyms=None):
        agg_perf_quality = ReportBinder(runner=self._runner, as_of_date=self._as_of_date,
                                        portfolio_acronyms=portfolio_acronyms)
        agg_perf_quality.execute()

    def agg_perf_quality_by_portfolio(self, portfolio_acronyms=None):
        agg_perf_quality = AggregatePerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date,
                                                             acronyms=portfolio_acronyms)
        agg_perf_quality.execute()


if __name__ == "__main__":
    report_runner = RunPerformanceQualityReports(as_of_date=dt.date(2022, 3, 31))
    # funds_and_peers = report_runner.generate_report_data(investment_ids=None)

    # funds_and_peers = json.loads(funds_and_peers)
    # fund_names = funds_and_peers.get('fund_names')
    # peer_groups = funds_and_peers.get('peer_groups')
    #
    # report_runner.generate_peer_summaries(peer_groups=peer_groups)
    # report_runner.generate_fund_reports(fund_names=['Citadel'])
    report_runner.agg_perf_quality_by_portfolio(portfolio_acronyms=['GIP'])
    # report_runner.combine_by_portfolio()

    # TODO - Update Abs Benchmark Returns
    # TODO - Add fns to template
    # TODO - Properly tag excel meta data for InvestmentGroup and Portfolio Excels
    # TODO - copy excel meta data to pdfs
    # TODO - generate cross asset meta data for aggregates

    # Next up
    # TODO Add portfolios to azure function
    # TODO add folder structure to data lake dumps/pass in file paths.
    # TODO run new RBA
    # TODO add strategy aggregations
    # TODO add to Data pipelines imports of RBA, PBA, and Abs Benchmark Returns
    # TODO populate HYG
    # TODO icelandic method
    # TODO review excess aggregation assumption w/ amy (i.e. Fund returns without
    #  Excess Return at portfolio level doesnt match Fund minus Benchmark Abs Bmrk returns)
    # TODO incorporate macro model
    # TODO run peer group stats only once
    # TODO update report tagging/write to reporting hub
    # TODO check Aspex RBA/compounding
    # TODO Add asofdate to params
    # TODO Document report and data provider (document default waterfall logic)
