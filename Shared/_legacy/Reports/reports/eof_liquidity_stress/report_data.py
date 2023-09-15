from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.Utils.tabular_data_util_outputs import TabularDataOutputTypes
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.daos.azure_datalake.azure_datalake_file import AzureDataLakeFile
from gcm.inv.dataprovider.factor import Factor
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
import pandas as pd


class EofStressTestingData(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, scenario: list):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._scenario = scenario
        self._runner = runner
        factor = Factor(tickers=[])
        self._factors = factor

    def eof_liquidity_stress_report_inputs(self):
        file_name = "Correlated_factors.csv"
        folder = "eof"
        loc = "raw/investmentsreporting/underlyingdata/"
        location = f"{loc}/{folder}/"
        params = AzureDataLakeDao.create_get_data_params(
            location, file_name, True
        )
        file: AzureDataLakeFile = self._runner.execute(
            params=params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(params),
        )
        correlated_factors = file.to_tabular_data(
            TabularDataOutputTypes.PandasDataFrame, params
        )

        # extract factor shocks from the FactorShock table if the shock exists
        # for factors that doesnt have calculated shocks available but have
        # z-scores the shock will be factor vol (correct frequency) * factor z score

        factors_exposure = self._factors.get_factor_exposure(
            as_of_date=self._as_of_date
        )

        exposure_factor = Factor(tickers=factors_exposure['SourceTicker'].to_list())
        factors_shock = exposure_factor.get_factor_shock(
            scenario=self._scenario,
            as_of_date=self._as_of_date
        )
        # extract exposure by factors
        factor_exposure = pd.merge(factors_exposure[['PortfolioExposure', 'SourceTicker']], factors_shock, how='left', on='SourceTicker')
        return factor_exposure, correlated_factors

    def run(self, **kwargs):
        return self.eof_liquidity_stress_report_inputs()
