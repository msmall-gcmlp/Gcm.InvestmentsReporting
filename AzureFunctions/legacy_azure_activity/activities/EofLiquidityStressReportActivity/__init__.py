from datetime import datetime
from _legacy.Reports.reports.eof_liquidity_stress.report_data import (
    EofStressTestingData,
)
from _legacy.Reports.reports.eof_liquidity_stress.eof_liquidity_stress_report import (
    EofLiquidityReport,
)
from gcm.inv.scenario import Scenario


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    # config_params = {
    #     DaoRunnerConfigArgs.dao_global_envs.name: {
    #         DaoSource.InvestmentsDwh.name: {
    #             "Environment": "prd",
    #             "Subscription": "prd",
    #         }
    #     }
    # }

    if run == "RunEofLiquidityStress":
        with Scenario(as_of_date=as_of_date).context():
            input_data = EofStressTestingData(
                runner=Scenario.get_attribute("dao"),
                as_of_date=params["as_of_date"],
                scenario=["Liquidity Stress"],
            ).execute()
            # runner2 = DaoRunner(
            #     container_lambda=lambda b, i: b.config.from_dict(i),
            #     config_params=config_params,
            # )

            eof_liquidity = EofLiquidityReport(
                runner=Scenario.get_attribute("dao"),
                as_of_date=as_of_date,
                factor_inventory=input_data[0],
                manager_exposure=input_data[0],
                correlated_factors=input_data[1]
            )

        return eof_liquidity.execute()
