import datetime as dt
from gcm.inv.reporting.reports.performance_quality_report import PerformanceQualityReport
from gcm.inv.reporting.reports.performance_quality_report_data import PerformanceQualityReportData
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs


def main(requestBody) -> str:
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

    if run == "PerformanceQualityReport":
        return PerformanceQualityReport(
            runner=runner,
            as_of_date=dt.date(2021, 12, 31),
            params=params
        ).execute()

    elif run == "PerformanceQualityReportData":
        return PerformanceQualityReportData(
            runner=runner,
            start_date=dt.date(2020, 10, 1),
            end_date=dt.date(2021, 12, 31),
            as_of_date=dt.date(2021, 12, 31),
            params=params
        ).execute()
