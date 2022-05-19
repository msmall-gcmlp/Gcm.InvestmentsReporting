import datetime as dt
import json

from gcm.inv.reporting.reports.performance_quality_peer_summary_report import PerformanceQualityPeerSummaryReport
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs


def main(requestBody) -> str:
    requestBody = json.loads(requestBody)
    params = requestBody["params"]

    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.PubDwh.name: {
                "Environment": "uat",
                "Subscription": "nonprd",
            },
        }
    }

    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )

    as_of_date = dt.date(2022, 3, 31)
    peer_group = params['peer_group']

    return PerformanceQualityPeerSummaryReport(
        runner=runner,
        as_of_date=as_of_date,
        peer_group=peer_group).execute()
