from datetime import datetime
from _legacy.Reports.reports.basel.basel_report import (
    BaselReport,
)
from _legacy.Reports.reports.basel.basel_report_data import (
    BaselReportData,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs

from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeDao,
)
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
from gcm.Dao.DaoSources import DaoSource

from gcm.Dao.Utils.tabular_data_util_outputs import (
    TabularDataOutputTypes,
)


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    balancedate = params["balancedate"]
    portfolio_name = params["portfolio_name"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    balancedate = datetime.strptime(balancedate, "%Y-%m-%d").date()
    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            }
        }
    }
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )

    if run == "RunBaselReport":
        file_name = "truview_output.csv"
        mapping_file_name = "basel_mapping.csv"
        folder = "basel"
        loc = "raw/investmentsreporting/underlyingdata/"
        location = f"{loc}/{folder}/"
        params = AzureDataLakeDao.create_get_data_params(location, file_name, True)
        params_mapping = AzureDataLakeDao.create_get_data_params(location, mapping_file_name, True)
        file: AzureDataLakeFile = runner.execute(
            params=params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(params),
        )
        df = file.to_tabular_data(TabularDataOutputTypes.PandasDataFrame, params)

        mapping_file: AzureDataLakeFile = runner.execute(
            params=params_mapping,
            source=DaoSource.DataLake,
            operation=lambda dao, params_mapping: dao.get_data(params_mapping),
        )
        df_mapping = mapping_file.to_tabular_data(TabularDataOutputTypes.PandasDataFrame, params_mapping)
        runner2 = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params=config_params,
        )
        portfolio_allocation = BaselReportData(
            runner=runner2,
            as_of_date=balancedate,
            funds_exposure=df,
            portfolio=portfolio_name,
        ).execute()
        for investment_name in portfolio_allocation["InvestmentName"].unique():
            data_per_investment = portfolio_allocation[portfolio_allocation["InvestmentName"] == investment_name]
            BaselReport(
                runner=runner2,
                as_of_date=as_of_date,
                investment_name=investment_name,
                input_data=data_per_investment,
                mapping_to_template=df_mapping,
            ).execute()

        return 0
