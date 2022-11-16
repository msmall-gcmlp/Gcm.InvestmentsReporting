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

        factor = Factor(tickers=[])
        self._factors = factor

    def eof_liquidity_stress_report_inputs(self):
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
        return factor_exposure

    def run(self, **kwargs):
        return self.eof_liquidity_stress_report_inputs()
