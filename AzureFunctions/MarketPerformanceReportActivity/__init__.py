from pandas.tseries.offsets import BDay
from datetime import datetime
from Reports.reports.market_performance.report_data import (
    MarketPerformanceQualityReportData,
)
from Reports.reports.market_performance.market_performance_report import (
    MarketPerformanceReport,
)
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Scenario.scenario import Scenario
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
    asofdate = params["asofdate"]
    as_of_date = datetime.strptime(asofdate, "%Y-%m-%d").date()
    start_date = as_of_date - BDay(505)
    runner = DaoRunner()

    if run == "RunMarketPerformanceQualityReportData":
        file_name = "market_performance_tickers.csv"
        folder = "marketperformance"
        loc = "raw/investmentsreporting/underlyingdata/"
        location = f"{loc}/{folder}/"
        params = AzureDataLakeDao.create_get_data_params(location, file_name, True)

        file: AzureDataLakeFile = runner.execute(
            params=params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(params),
        )
        df = file.to_tabular_data(TabularDataOutputTypes.PandasDataFrame, params)
        with Scenario(runner=runner, as_of_date=as_of_date).context():
            input_data = MarketPerformanceQualityReportData(
                start_date=start_date,
                runner=runner,
                as_of_date=as_of_date,
                ticker_map=df,
            ).execute()
        runner2 = DaoRunner()
        MarketPerformance = MarketPerformanceReport(
            runner=runner2,
            as_of_date=as_of_date,
            interval="MTD",
            factor_daily_returns=input_data[0],
            prices=input_data[1],
            price_change=input_data[2],
            ticker_mapping=df,
        )

        return MarketPerformance.execute()
