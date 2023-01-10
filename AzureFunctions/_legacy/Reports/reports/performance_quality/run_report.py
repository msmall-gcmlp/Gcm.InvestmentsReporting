import datetime as dt
import json
from gcm.inv.scenario import Scenario
from pandas._libs.tslibs.offsets import relativedelta
from _legacy.Reports.reports.performance_quality.peer_level_analytics import PerformanceQualityPeerLevelAnalytics
from _legacy.Reports.reports.performance_quality.report_data import PerformanceQualityReportData
from _legacy.Reports.reports.performance_quality.report import PerformanceQualityReport
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource


class RunPerformanceQualityReports:
    def __init__(self, as_of_date):
        self._as_of_date = as_of_date
        self._params = {"status": "EMM", "vertical": "ARS", "entity": "PFUND"}

    def generate_report_data(self, investment_group_ids):
        perf_quality_data = PerformanceQualityReportData(
            start_date=self._as_of_date - relativedelta(years=10), end_date=self._as_of_date,
            investment_group_ids=investment_group_ids
        )
        return perf_quality_data.execute()

    def generate_peer_summaries(self, peer_groups):
        for peer in peer_groups:
            perf_quality_report = PerformanceQualityPeerLevelAnalytics(peer_group=peer)
            perf_quality_report.execute()

    def generate_fund_reports(self, fund_names):
        for fund in fund_names:
            perf_quality_report = PerformanceQualityReport(fund_name=fund)
            perf_quality_report.execute()


if __name__ == "__main__":
    runner = DaoRunner()
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.DataLake_Blob.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "uat",
                    "Subscription": "nonprd",
                },
            }
        },
    )
    for as_of_date in [dt.date(2022, 10, 31)]:
        with Scenario(dao=runner, as_of_date=as_of_date).context():
            report_runner = RunPerformanceQualityReports(as_of_date=as_of_date)
            prd_ids = [20016, 23441, 75614, 28015]
            # dev_ids = [19224, 23319, 74984]

            funds_and_peers = report_runner.generate_report_data(investment_group_ids=[18506])

            funds_and_peers = json.loads(funds_and_peers)
            fund_names = funds_and_peers.get("fund_names")
            peer_groups = funds_and_peers.get("peer_groups")

            report_runner.generate_peer_summaries(peer_groups=peer_groups)
            report_runner.generate_fund_reports(fund_names=fund_names)
