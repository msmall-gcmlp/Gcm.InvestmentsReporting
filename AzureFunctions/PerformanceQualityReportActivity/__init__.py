import datetime as dt
import json
from gcm.inv.reporting.reports.performance_quality_report import PerformanceQualityReport
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
    fund_name = params['fund_name']

    PerformanceQualityReport(
        runner=runner,
        as_of_date=as_of_date,
        fund_name=fund_name
    ).execute()
