from datetime import datetime as dt
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.eof_risk_report.eof_external_report import (
    EofExternalData)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource

if __name__ == "__main__":
    as_of_date = "2023-08-15"
    scenario = ["EOF External"]
    as_of_date = dt.strptime(as_of_date, "%Y-%m-%d").date()

    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                }
            },
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.ReportingStorage.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
        },
    )

    runner_pub = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                }
            },
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.ReportingStorage.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
        },
    )

    with Scenario(dao=runner, as_of_date=as_of_date).context():
        input_data = EofExternalData(
            runner=runner,
            as_of_date=as_of_date,
            scenario=["EOF External"],
        ).execute()
