import datetime as dt
import glob
import json
from gcm.inv.scenario import Scenario
from pandas._libs.tslibs.offsets import relativedelta
from _legacy.core.Utils.aggregate_file_utils import copy_metadata
from _legacy.Reports.reports.performance_quality.aggregate_performance_quality_report import AggregatePerformanceQualityReport
from _legacy.Reports.reports.performance_quality.performance_quality_peer_summary_report import PerformanceQualityPeerSummaryReport
from _legacy.Reports.reports.performance_quality.performance_quality_report_data import PerformanceQualityReportData
from _legacy.Reports.reports.performance_quality.performance_quality_report import PerformanceQualityReport
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Dao.DaoSources import DaoSource
from _legacy.Reports.reports.report_binder import ReportBinder


class RunPerformanceQualityReports:
    def __init__(self, as_of_date):
        self._runner = DaoRunner()
        # self._runner = DaoRunner(
        #     container_lambda=lambda b, i: b.config.from_dict(i),
        #     config_params={
        #         DaoRunnerConfigArgs.dao_global_envs.name: {
        #             DaoSource.DataLake.name: {
        #                 "Environment": "prd",
        #                 "Subscription": "prd",
        #             },
        #             DaoSource.PubDwh.name: {
        #                 "Environment": "prd",
        #                 "Subscription": "prd",
        #             },
        #             DaoSource.InvestmentsDwh.name: {
        #                 "Environment": "prd",
        #                 "Subscription": "prd",
        #             },
        #             DaoSource.DataLake_Blob.name: {
        #                 "Environment": "prd",
        #                 "Subscription": "prd",
        #             },
        #         }
        #     },
        # )
        self._as_of_date = as_of_date
        self._params = {"status": "EMM", "vertical": "ARS", "entity": "PFUND"}

    def generate_report_data(self, investment_group_ids):
        with Scenario(runner=self._runner, as_of_date=self._as_of_date).context():
            perf_quality_data = PerformanceQualityReportData(
                start_date=self._as_of_date - relativedelta(years=10), end_date=self._as_of_date, investment_group_ids=investment_group_ids
            )
            return perf_quality_data.execute()

    def generate_peer_summaries(self, peer_groups):
        for peer in peer_groups:
            perf_quality_report = PerformanceQualityPeerSummaryReport(runner=self._runner, as_of_date=self._as_of_date, peer_group=peer)
            perf_quality_report.execute()

    def generate_fund_reports(self, fund_names):
        for fund in fund_names:
            perf_quality_report = PerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date, fund_name=fund)
            perf_quality_report.execute()

    def combine_by_portfolio(self, portfolio_acronyms=None):
        agg_perf_quality = ReportBinder(runner=self._runner, as_of_date=self._as_of_date, portfolio_acronyms=portfolio_acronyms)
        agg_perf_quality.execute()

    def agg_perf_quality_by_portfolio(self, portfolio_acronyms=None):
        with Scenario(runner=self._runner, as_of_date=self._as_of_date).context():
            agg_perf_quality = AggregatePerformanceQualityReport(runner=self._runner, as_of_date=self._as_of_date, acronyms=portfolio_acronyms)
            agg_perf_quality.execute()

    def copy_meta_data_from_excels(self):
        file_path = "C:/Users/CMCNAMEE/OneDrive - GCM Grosvenor/Desktop/tmp"
        files = glob.glob(file_path + "/*.pdf")

        file_path = file_path.removesuffix("\\") + "\\"
        file_names = [x.removeprefix(file_path).removesuffix(".pdf") for x in files]

        for file in file_names:
            copy_metadata(
                runner=self._runner,
                target_file_location="performance/Risk",
                target_file_name=file + ".pdf",
                target_dao_type=DaoSource.ReportingStorage,
                source_file_location="performance/Risk",
                source_file_name=file + ".xlsx",
                source_dao_type=DaoSource.ReportingStorage,
            )

    def copy_portfolio_meta_data(self):
        file_path = "C:/Users/CMCNAMEE/OneDrive - GCM Grosvenor/Desktop/tmp"
        files = glob.glob(file_path + "/*.pdf")
        files = [k for k in files if "PORTFOLIO_Risk" in k]
        files = [k for k in files if "AllActive" not in k]

        file_path = file_path.removesuffix("\\") + "\\"
        file_names = [x.removeprefix(file_path).removesuffix(".pdf") for x in files]

        for file in file_names:
            source_file = file + ".pdf"
            target_file = source_file.replace("PORTFOLIO", "FundAggregate")
            copy_metadata(
                runner=self._runner,
                target_file_location="performance/Risk",
                target_file_name=target_file,
                target_dao_type=DaoSource.ReportingStorage,
                source_file_location="performance/Risk",
                source_file_name=source_file,
                source_dao_type=DaoSource.ReportingStorage,
            )


if __name__ == "__main__":
    report_runner = RunPerformanceQualityReports(as_of_date=dt.date(2022, 4, 30))
    funds_and_peers = report_runner.generate_report_data(investment_group_ids=[19224, 23319, 74984])

    funds_and_peers = json.loads(funds_and_peers)
    fund_names = funds_and_peers.get("fund_names")
    peer_groups = funds_and_peers.get("peer_groups")

    report_runner.generate_peer_summaries(peer_groups=peer_groups)

    report_runner.generate_fund_reports(fund_names=["Skye", "Citadel", "D1 Capital"])
    # report_runner.agg_perf_quality_by_portfolio(portfolio_acronyms=['IFC'])
    # manually convert all individual excels to pdf
    # report_runner.copy_meta_data_from_excels()
    # report_runner.combine_by_portfolio()
    # manually drop FundAggregates and AllActive summaries in ReportingHub UAT
    # report_runner.copy_portfolio_meta_data()
    # manually apply meta-data to All Portfolio packet
    # manually apply meta-data to All Fund packet

    # TODO copy *FundAggregate_Risk_2022-03-31.pdf* from UAT to prod
    # TODO copy *PFUND_Risk_2022-03-31.pdf* from UAT to prod
    # TODO copy *AllActive_PFUND_Risk_2022-03-31.pdf* from UAT to prod
    # TODO copy *AllActive_PORTFOLIO_Risk_2022-03-31.pdf* from UAT to prod

    # Next up

    # TODO Add portfolios to azure function
    # TODO schedule RBA (macro and equity)
    # TODO add strategy aggregations
    # TODO add to Data pipelines imports of RBA, PBA, and Abs Benchmark Returns
