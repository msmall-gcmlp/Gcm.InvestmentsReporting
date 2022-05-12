import datetime as dt
import json
from gcm.inv.reporting.reports.performance_quality_report import PerformanceQualityReport
from gcm.inv.reporting.reports.performance_quality_report_data import PerformanceQualityReportData
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs
from dateutil.relativedelta import relativedelta


def main(requestBody) -> str:
    requestBody = json.loads(requestBody)
    params = requestBody["params"]
    # data = requestBody["data"]
    run = params["run"]
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

    if run == "PerformanceQualityReportData":

        return PerformanceQualityReportData(
            runner=runner,
            start_date=as_of_date - relativedelta(years=10),
            end_date=as_of_date,
            as_of_date=as_of_date,
            params=params
        ).execute()

    elif run == "PerformanceQualityReport":

        return PerformanceQualityReport(
            runner=runner,
            as_of_date=as_of_date,
            params=params
        ).execute()
