import datetime as dt
from gcm.Scenario.scenario import Scenario
from Reports.reports.performance_screener_report import PerformanceScreenerReport
from gcm.Dao.DaoRunner import DaoRunner


class RunPerformanceScreenerReport:

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

    def run_performance_screener(self, peer_groups):
        with Scenario(runner=self._runner, as_of_date=self._as_of_date).context():
            for peer_group in peer_groups:
                peer_ranking = PerformanceScreenerReport(peer_group=peer_group)
                peer_ranking.execute()
        return True


if __name__ == "__main__":
    report_runner = RunPerformanceScreenerReport(as_of_date=dt.date(2022, 6, 30))
    report_runner.run_performance_screener(peer_groups=['GCM Multi-PM',
                                                        'GCM Equities',
                                                        'GCM Credit',
                                                        'GCM Diversifying Strategies',
                                                        'GCM Macro',
                                                        'GCM TMT'])
