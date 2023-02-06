import datetime as dt
from functools import cached_property
import pandas as pd
from _legacy.Reports.reports.performance_quality.fund_distribution_simulator.passive_benchmark_returns import generate_arb_simulations
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario
import numpy as np
import os


class SimulateResiduals(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    @cached_property
    def _raw_peer_cor_mat(self):
        cor_mat = pd.read_csv(os.path.dirname(__file__) + "/expected_peer_cor_mat.csv")
        return cor_mat

    @cached_property
    def _raw_fund_cor_mat(self):
        cor_mat = pd.read_csv(os.path.dirname(__file__) + "/expected_fund_excess_cor_mat.csv")
        return cor_mat

    @cached_property
    def _ordered_raw_peer_names(self):
        return self._raw_peer_cor_mat.columns[1:]

    @cached_property
    def _ordered_raw_fund_names(self):
        return self._raw_fund_cor_mat.columns[1:]

    @cached_property
    def _peer_arb_mapping(self):
        return pd.read_csv(os.path.dirname(__file__) + "/../peer_group_to_arb_mapping.csv")

    @cached_property
    def _peer_short_name_mapping(self):
        return dict(zip(self._peer_arb_mapping['PeerGroupShortName'], self._peer_arb_mapping['ReportingPeerGroup']))

    @cached_property
    def _peer_expectations(self):
        expectations = pd.read_csv(os.path.dirname(__file__) + "/peer_residual_expectations.csv")
        expectations = expectations.set_index('PeerShortName').reindex(self._ordered_raw_peer_names)
        expectations.index = expectations.index.map(self._peer_short_name_mapping)
        return expectations

    @cached_property
    def _fund_expectations(self):
        expectations = pd.read_csv(os.path.dirname(__file__) + "/fund_residual_expectations.csv")
        expectations = expectations.set_index('Fund').reindex(self._ordered_raw_fund_names)
        return expectations

    @staticmethod
    def _generate_monthly_residual_simulations(cor_mat, names, expectations, number_sims=10_000):
        i_upper = np.triu_indices(cor_mat.shape[0], 1)
        cor_mat[i_upper] = cor_mat.T[i_upper]
        cor_mat = pd.DataFrame(cor_mat)
        cor_mat.index = names
        cor_mat.columns = names

        means = [0] * len(names)

        vols = expectations['ResidualVol'].tolist()
        vols = [(x * 3 * np.sqrt(1 / 36)).round(5) for x in vols]

        cov_mat = np.asarray(cor_mat) * np.outer(vols, vols)

        residuals = np.random.default_rng().multivariate_normal(means, cov_mat, number_sims, check_valid='ignore')
        residuals = pd.DataFrame(residuals, columns=names, index=[x for x in range(number_sims)])

        all(residuals.mean().round(2) == means)
        assert (residuals.std().round(2) - [x.round(2) for x in vols]).mean().round(2) == 0
        all(residuals.corr().round(1) == cor_mat.round(1))
        return residuals

    def _generate_monthly_peer_residual_simulations(self, number_sims=10_000):
        cor_mat = np.asarray(self._raw_peer_cor_mat.iloc[:, 1:])
        raw_peers = self._ordered_raw_peer_names
        peers = raw_peers.map(self._peer_short_name_mapping)
        residuals = self._generate_monthly_residual_simulations(cor_mat=cor_mat,
                                                                names=peers,
                                                                expectations=self._peer_expectations,
                                                                number_sims=number_sims)
        return residuals

    def _generate_monthly_fund_residual_simulations(self, number_sims=10_000):
        cor_mat = np.asarray(self._raw_fund_cor_mat.iloc[:, 1:])
        residuals = self._generate_monthly_residual_simulations(cor_mat=cor_mat,
                                                                names=self._ordered_raw_fund_names,
                                                                expectations=self._fund_expectations,
                                                                number_sims=number_sims)
        return residuals

    @staticmethod
    def _generate_rolling_residual_simulations(monthly_residuals, rolling_months):
        rolling_residuals = monthly_residuals.rolling(rolling_months).sum() / 3
        rolling_residuals = rolling_residuals.dropna().reset_index(drop=True)
        return rolling_residuals

    def _generate_rolling_peer_residual_simulations(self, rolling_years=3, number_sims=10_000):
        rolling_months = int(rolling_years * 12)
        n_sims = number_sims + rolling_months - 1
        monthly_residuals = self._generate_monthly_peer_residual_simulations(number_sims=n_sims)
        rolling_residuals = self._generate_rolling_residual_simulations(monthly_residuals=monthly_residuals,
                                                                        rolling_months=rolling_months)

        return rolling_residuals

    def _generate_rolling_fund_residual_simulations(self, rolling_years=3, number_sims=10_000):
        rolling_months = int(rolling_years * 12)
        n_sims = number_sims + rolling_months - 1
        monthly_residuals = self._generate_monthly_fund_residual_simulations(number_sims=n_sims)
        rolling_residuals = self._generate_rolling_residual_simulations(monthly_residuals=monthly_residuals,
                                                                        rolling_months=rolling_months)

        return rolling_residuals

    def generate_rolling_peer_excess_simulations(self, arb_sims, rolling_years=3, number_sims=10_000):
        error = self._generate_rolling_peer_residual_simulations(rolling_years=rolling_years, number_sims=number_sims)
        excess = error.copy()
        for peer in error.columns:
            exp = self._peer_expectations.loc[peer]
            alpha = exp.loc['Excess_50th']
            beta = exp.loc['BetaToArb']
            arb = self._peer_arb_mapping[self._peer_arb_mapping['ReportingPeerGroup'] == peer]['Ticker']
            if peer == 'GCM Macro':
                beta = beta / 100
                arb = 'MOVE Index'
            excess[peer] = (beta * arb_sims[arb]).squeeze() + alpha + error[peer]

        return excess

    def _generate_fund_alpha_draw_percentile_draw(self, rolling_years=3, number_sims=10_000):
        # n_months = int(rolling_years * 12)
        #
        # # each monthly alpha is a random draw
        # monthly_draw = pd.DataFrame([random.random() for x in range(number_sims + n_months - 1)])
        #
        # np.random.normal(0.05, 0.1, 100)
        #
        # # over rolling 3-year period, total alpha is rolling average of monthly draws
        # # rolling_draw represents the percentile of alpha from alpha distribution
        # rolling_draw = monthly_draw.rolling(n_months).mean().dropna().rank(pct=True)
        return True

    def generate_rolling_fund_excess_simulations(self, arb_sims, peer_sims, rolling_years=3, number_sims=10_000):
        error = self._generate_rolling_fund_residual_simulations(rolling_years=rolling_years, number_sims=number_sims)
        excess = error.copy()
        for fund in error.columns:
            exp = self._fund_expectations.loc[fund]
            peer_group = exp.loc['PeerGroup']
            peer_exp = self._peer_expectations.loc[peer_group]
            peer_alphas = peer_exp.loc[['Excess_90th',
                                        'Excess_75th',
                                        'Excess_50th',
                                        'Excess_25th',
                                        'Excess_10th']]

            arb = self._peer_arb_mapping[self._peer_arb_mapping['ReportingPeerGroup'] == peer_group]['Ticker']

            market_beta_ctr = (exp.loc['NetNotional'] * arb_sims[arb]).squeeze()
            peer_beta_ctr = (exp.loc['BetaToPeer'] * peer_sims[peer_group]).squeeze()
            fund_error = error[fund]
            fund_alpha = peer_alphas.loc['Excess_' + str(exp.loc['ExcessPtile']) + 'th']
            excess[fund] = market_beta_ctr + peer_beta_ctr + fund_alpha + fund_error

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
        fund_sims = SimulateResiduals().generate_rolling_fund_excess_simulations(arb_sims=arb_sims,
                                                                                 peer_sims=peer_sims)
        sims = pd.concat([arb_sims, peer_sims], axis=1)
        sim_cor_mat = sims.corr().round(1)
        sims.mean()
        sims.std()
