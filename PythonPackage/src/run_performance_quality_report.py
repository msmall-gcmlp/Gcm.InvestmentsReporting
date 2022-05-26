import datetime as dt
import json
import glob
from pandas._libs.tslibs.offsets import relativedelta
from gcm.inv.reporting.core.Utils.aggregate_file_utils import copy_metadata
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
                    DaoSource.ReportingStorage.name: {
                        "Environment": "uat",
                        "Subscription": "nonprd",
                    }
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
        perf_quality_report = PerformanceQualityPeerSummaryReport(runner=self._runner, as_of_date=self._as_of_date,
                                                                  peer_group=None)
        for peer in peer_groups:
            perf_quality_report._peer_group = peer
            perf_quality_report.execute()

    def generate_fund_reports(self, fund_names):
        perf_quality_report = PerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date,
                                                       fund_name=None)
        for fund in fund_names:
            perf_quality_report._fund_name = fund
            perf_quality_report.execute()

    def combine_by_portfolio(self, portfolio_acronyms=None):
        agg_perf_quality = ReportBinder(runner=self._runner, as_of_date=self._as_of_date,
                                        portfolio_acronyms=portfolio_acronyms)
        agg_perf_quality.execute()

    def agg_perf_quality_by_portfolio(self, portfolio_acronyms=None):
        agg_perf_quality = AggregatePerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date,
                                                             acronyms=portfolio_acronyms)
        agg_perf_quality.execute()

    def copy_meta_data_from_excels(self):
        file_path = "C:/Users/CMCNAMEE/OneDrive - GCM Grosvenor/Desktop/tmp"
        files = glob.glob(file_path + "/*.pdf")

        file_path = file_path.removesuffix('\\') + '\\'
        file_names = [x.removeprefix(file_path).removesuffix('.pdf') for x in files]

        for file in file_names:
            copy_metadata(runner=self._runner,
                          target_file_location='performance/Risk',
                          target_file_name=file + '.pdf',
                          target_dao_type=DaoSource.ReportingStorage,
                          source_file_location='performance/Risk',
                          source_file_name=file + '.xlsx',
                          source_dao_type=DaoSource.ReportingStorage,
                          )

    def copy_portfolio_meta_data(self):
        file_path = "C:/Users/CMCNAMEE/OneDrive - GCM Grosvenor/Desktop/tmp"
        files = glob.glob(file_path + "/*.pdf")
        files = [k for k in files if 'PORTFOLIO_Risk' in k]
        files = [k for k in files if 'AllActive' not in k]

        file_path = file_path.removesuffix('\\') + '\\'
        file_names = [x.removeprefix(file_path).removesuffix('.pdf') for x in files]

        for file in file_names:
            source_file = file + '.pdf'
            target_file = source_file.replace('PORTFOLIO', 'FundAggregate')
            copy_metadata(runner=self._runner,
                          target_file_location='performance/Risk',
                          target_file_name=target_file,
                          target_dao_type=DaoSource.ReportingStorage,
                          source_file_location='performance/Risk',
                          source_file_name=source_file,
                          source_dao_type=DaoSource.ReportingStorage,
                          )


if __name__ == "__main__":
    report_runner = RunPerformanceQualityReports(as_of_date=dt.date(2022, 3, 31))
    funds_and_peers = report_runner.generate_report_data(investment_ids=None)

    funds_and_peers = json.loads(funds_and_peers)
    fund_names = funds_and_peers.get('fund_names')
    peer_groups = funds_and_peers.get('peer_groups')

    report_runner.generate_peer_summaries(peer_groups=peer_groups)
    report_runner.generate_fund_reports(fund_names=['Skye'])
    report_runner.agg_perf_quality_by_portfolio(portfolio_acronyms=['IFC'])
    # TODO convert all individual excels to pdf
    # TODO for all file names in directory, apply metadata from pdf to excel
    report_runner.copy_meta_data_from_excels()
    report_runner.combine_by_portfolio()
    # TODO MANUAL: drop FundAggregates and AllActive summaries in ReportingHub UAT
    report_runner.copy_portfolio_meta_data()

    # TODO apply meta data to All Portfolio packet
    # TODO apply meta data to All Fund packet

    # TODO copy *FundAggregate_Risk_2022-03-31.pdf* from UAT to prod
    # TODO copy *PFUND_Risk_2022-03-31.pdf* from UAT to prod
    # TODO copy *AllActive_PFUND_Risk_2022-03-31.pdf* from UAT to prod
    # TODO copy *AllActive_PORTFOLIO_Risk_2022-03-31.pdf* from UAT to prod

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
