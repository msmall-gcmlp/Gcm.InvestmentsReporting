import datetime as dt
from gcm.Scenario.scenario import Scenario
from Reports.reports.peer_ranking_report import PeerRankingReport
from gcm.Dao.DaoRunner import DaoRunner


class RunPeerRankingReport:

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

    def run_peer_rankings(self, peer_groups):
        with Scenario(runner=self._runner, as_of_date=self._as_of_date).context():
            for peer_group in peer_groups:
                peer_ranking = PeerRankingReport(peer_group=peer_group)
                peer_ranking.execute()
        return True


if __name__ == "__main__":
    report_runner = RunPeerRankingReport(as_of_date=dt.date(2022, 4, 30))
    report_runner.run_peer_rankings(peer_groups=['GCM TMT'])
