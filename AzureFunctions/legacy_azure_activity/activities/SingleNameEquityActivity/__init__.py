from datetime import datetime
from gcm.inv.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from _legacy.Reports.reports.risk_model.single_name_exposure import SingleNameReport
from gcm.Dao.DaoSources import DaoSource


def main(requestBody) -> str:
    params = requestBody["params"]
    as_of_date = params["as_of_date"]

    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                # DaoSource.DataLake.name: {
                #     "Subscription": "prd"},
            }
        },
    )

    with Scenario(runner=runner, as_of_date=as_of_date).context():
        single_name_equity_exposure = SingleNameReport()
        return single_name_equity_exposure.execute()