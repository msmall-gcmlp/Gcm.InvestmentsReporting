import datetime as dt
from functools import cached_property
from scipy.stats import spearmanr
import pandas as pd
import numpy as np
import pandas as pd
from _legacy.Reports.reports.performance_quality.fund_distribution_simulator.passive_benchmark_returns import \
    get_monthly_returns, query_historical_benchmark_returns, summarize_benchmark_returns, get_arb_benchmark_summary, \
    generate_arb_simulations
from _legacy.Reports.reports.performance_quality.peer_conditional_excess_returns import calculate_rolling_excess_returns
from _legacy.core.ReportStructure.report_structure import ReportingEntityTypes, ReportType, ReportVertical
from _legacy.core.Runners.investmentsreporting import InvestmentsReportRunner
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario
import numpy as np
import scipy
from gcm.inv.dataprovider.peer_group import PeerGroup
import os
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from gcm.inv.dataprovider.investment_group import InvestmentGroup


class SimulateResiduals(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    @cached_property
    def _raw_cor_mat(self):
        cor_mat = pd.read_csv(os.path.dirname(__file__) + "/expected_peer_cor_mat.csv")
        return cor_mat

    @cached_property
    def _ordered_raw_peer_names(self):
        return self._raw_cor_mat.columns[1:]

    @cached_property
    def _peer_arb_mapping(self):
        return pd.read_csv(os.path.dirname(__file__) + "/../peer_group_to_arb_mapping.csv")

    @cached_property
    def _peer_short_name_mapping(self):
        return dict(zip(self._peer_arb_mapping['PeerGroupShortName'], self._peer_arb_mapping['ReportingPeerGroup']))

    @cached_property
    def _expectations(self):
        expectations = pd.read_csv(os.path.dirname(__file__) + "/peer_residual_expectations.csv")
        expectations = expectations.set_index('PeerShortName').reindex(self._ordered_raw_peer_names)
        expectations.index = expectations.index.map(self._peer_short_name_mapping)
        return expectations

    def _generate_monthly_peer_residual_simulations(self, number_sims=10_000):
        cor_mat = self._raw_cor_mat
        raw_peers = self._ordered_raw_peer_names

        peers = raw_peers.map(self._peer_short_name_mapping)

        cor_mat = np.asarray(cor_mat.iloc[:, 1:])

        i_upper = np.triu_indices(cor_mat.shape[0], 1)
        cor_mat[i_upper] = cor_mat.T[i_upper]
        cor_mat = pd.DataFrame(cor_mat)
        cor_mat.index = peers
        cor_mat.columns = peers

        means = [0] * len(peers)

        vols = self._expectations['ResidualVol'].tolist()
        vols = [(x * 3 * np.sqrt(1 / 36)).round(5) for x in vols]

        cov_mat = np.asarray(cor_mat) * np.outer(vols, vols)

        residuals = np.random.default_rng().multivariate_normal(means, cov_mat, number_sims, check_valid='ignore')
        residuals = pd.DataFrame(residuals, columns=peers, index=[x for x in range(number_sims)])

        all(residuals.mean().round(2) == means)
        assert (residuals.std().round(2) - [x.round(2) for x in vols]).mean().round(2) == 0
        all(residuals.corr().round(1) == cor_mat.round(1))
        return residuals

    def _generate_rolling_peer_residual_simulations(self, rolling_years=3, number_sims=10_000):
        rolling_months = int(rolling_years * 12)
        n_sims = number_sims + rolling_months - 1
        monthly_residuals = self._generate_monthly_peer_residual_simulations(number_sims=n_sims)
        rolling_residuals = monthly_residuals.rolling(rolling_months).sum() / 3
        rolling_residuals = rolling_residuals.dropna().reset_index(drop=True)
        return rolling_residuals

    def generate_rolling_peer_excess_simulations(self, arb_sims, rolling_years=3, number_sims=10_000):
        error = self._generate_rolling_peer_residual_simulations(rolling_years=rolling_years, number_sims=number_sims)
        excess = error.copy()
        for peer in error.columns:
            exp = self._expectations.loc[peer]
            alpha = exp.loc['AlphaToArb']
            beta = exp.loc['BetaToArb']
            arb = self._peer_arb_mapping[self._peer_arb_mapping['ReportingPeerGroup'] == peer]['Ticker']
            if peer == 'GCM Macro':
                beta = beta / 100
                arb = 'MOVE Index'
            excess[peer] = (beta * arb_sims[arb]).squeeze() + alpha + error[peer]

        return excess

    def run(self, **kwargs):
        return True


if __name__ == "__main__":
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.DataLake_Blob.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

    as_of_date = dt.date(2022, 12, 31)

    with Scenario(dao=runner, as_of_date=as_of_date).context():
        arb_sims = generate_arb_simulations(exp_rf=0.03, as_of_date=as_of_date)
        peer_sims = SimulateResiduals().generate_rolling_peer_excess_simulations(arb_sims=arb_sims)
        sims = pd.concat([arb_sims, peer_sims], axis=1)
        sim_cor_mat = sims.corr().round(1)
        sims.mean()
        sims.std()
