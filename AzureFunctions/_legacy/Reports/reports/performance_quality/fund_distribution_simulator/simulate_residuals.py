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
from gcm.inv.models.alpha_percentile_conviction.sampling import sample_alpha
import numpy as np
import os
import scipy


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
    def _generate_monthly_residual_simulations(residual_vols, cor_mat, names, number_sims=10_000):
        i_upper = np.triu_indices(cor_mat.shape[0], 1)
        cor_mat[i_upper] = cor_mat.T[i_upper]
        cor_mat = pd.DataFrame(cor_mat)
        cor_mat.index = names
        cor_mat.columns = names

        means = [0] * len(names)
        vols = residual_vols
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
        res_vols = self._peer_expectations['VolExSystematic'].tolist()
        residuals = self._generate_monthly_residual_simulations(cor_mat=cor_mat,
                                                                names=peers,
                                                                residual_vols=res_vols,
                                                                number_sims=number_sims)
        return residuals

    def _generate_monthly_fund_residual_simulations(self, number_sims=10_000):
        cor_mat = np.asarray(self._raw_fund_cor_mat.iloc[:, 1:])
        peers = self._fund_expectations['PeerGroup'].loc[self._raw_fund_cor_mat['Fund']]
        for a in range(peers.shape[0]):
            for b in range(peers.shape[0]):
                if peers[a] == peers[b]:
                    cor_mat[a, b] = max(cor_mat[a, b], 0.30)
                else:
                    cor_mat[a, b] = max(cor_mat[a, b], 0.10)

        exp = self._fund_expectations
        peer_vol = self._peer_expectations['VolExSystematic']
        peer_vol = peer_vol.loc[exp['PeerGroup']]
        fund_vol = exp['VolExSystematic']
        beta = exp['CorrToPeer'] * (fund_vol.values / peer_vol.values)
        idio_vol = np.sqrt((fund_vol ** 2).values - ((beta ** 2).values * (peer_vol.values ** 2)))
        # exp['ResidualVol'] = pd.DataFrame(idio_vol, index=fund_vol.index)
        residual_vols = idio_vol.tolist()
        residuals = self._generate_monthly_residual_simulations(cor_mat=cor_mat,
                                                                names=self._ordered_raw_fund_names,
                                                                residual_vols=residual_vols,
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

    @staticmethod
    def _generate_rolling_index(number_sims, n_months):
        # uniform draw of monthly indexes
        independent_monthly_draws = pd.DataFrame([np.random.random() for x in range(number_sims + n_months - 1)])
        rolling_index = independent_monthly_draws.rolling(n_months).mean().dropna().rank().astype(int)
        rolling_index = rolling_index.squeeze().tolist()
        return rolling_index

    def _simulate_fund_excess_to_arb(self, fund_excess_ptile, peer_excess_grid, conviction=3,
                                     rolling_years=3, number_sims=10_000, conf_level_cap=0.80):
        n_months = int(rolling_years * 12)

        independent_alphas = sample_alpha(
            fund_ptile_mode=fund_excess_ptile,
            conviction_level=conviction,
            peer_alpha_25th=peer_excess_grid.loc['Excess_25th'],
            peer_alpha_75th=peer_excess_grid.loc['Excess_75th'],
            max_alpha=(peer_excess_grid.loc['Excess_90th'] + 0.05),
            min_alpha=(peer_excess_grid.loc['Excess_10th'] - 0.05),
            num_samples=number_sims + n_months - 1,
            conf_level_cap=conf_level_cap)
        independent_alphas = pd.DataFrame(sorted(independent_alphas))

        rolling_index = self._generate_rolling_index(number_sims=number_sims, n_months=n_months)

        rolling_alphas = independent_alphas.iloc[rolling_index, :]
        rolling_alphas = rolling_alphas.reset_index(drop=True).squeeze()

        return rolling_alphas

    def generate_rolling_fund_excess_simulations(self, arb_sims, peer_sims, exp_rf, rolling_years=3, number_sims=10_000):
        error = self._generate_rolling_fund_residual_simulations(rolling_years=rolling_years, number_sims=number_sims)
        excess = error.copy()
        for fund in error.columns:
            print(fund)
            exp = self._fund_expectations.loc[fund]
            peer_group = exp.loc['PeerGroup']
            peer_exp = self._peer_expectations.loc[peer_group]
            peer_alphas = peer_exp.loc[['Excess_90th',
                                        'Excess_75th',
                                        'Excess_50th',
                                        'Excess_25th',
                                        'Excess_10th']]

            arb = self._peer_arb_mapping[self._peer_arb_mapping['ReportingPeerGroup'] == peer_group]['Ticker']

            market_beta_ctr = (exp.loc['NetNotional'] * (arb_sims[arb] - exp_rf)).squeeze()
            beta_to_peer = exp.loc['CorrToPeer'] * (exp.loc['VolExSystematic'] / peer_exp.loc['VolExSystematic'])
            peer_beta_ctr = (beta_to_peer * peer_sims[peer_group]).squeeze()
            fund_error = error[fund]

            fund_excess = self._simulate_fund_excess_to_arb(fund_excess_ptile=exp.loc['ExcessPtile'] / 100,
                                                            peer_excess_grid=peer_alphas,
                                                            conviction=exp.loc['Conviction'])

            # fund_excess includes benefits of exposure to premia embedded in peer group.
            # need to subtract the peer beta ctr from the excess to arrive at fund-specific delta
            fund_alpha = fund_excess - peer_beta_ctr.mean()
            excess[fund] = market_beta_ctr + peer_beta_ctr + fund_alpha + fund_error + exp_rf

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
                                                                                 peer_sims=peer_sims,
                                                                                 exp_rf=0.03)
        sims = pd.concat([arb_sims, peer_sims, fund_sims], axis=1)
        sim_cor_mat = sims.corr().round(1)

        fund_sim_cor_mat = fund_sims.corr().round(1)
        sim_summary = pd.DataFrame({'Return': fund_sims.mean(),
                      'Vol': fund_sims.std() / np.sqrt(1 / 3),
                      'Sharpe': (fund_sims.mean() - 0.03) / (fund_sims.std() / np.sqrt(1 / 3)),
                      'AvgCorr': (fund_sim_cor_mat.sum() - 1) / (fund_sim_cor_mat.shape[0] - 1)})

        scipy.stats.linregress(y=fund_sims.mean(axis=1), x=arb_sims['SPXT Index'])

        sim_summary.to_csv('sim_summary.csv')

        gcm_weights = pd.read_csv('gcm_firmwide_weights.csv')
        exp_return = 0.3 * (0.09 - 0.03) + 0.04 + 0.03
        exp_vol = 0.07
        exp_rf = 0.03
        lam = exp_return / (exp_vol ** 2)
        implied_returns = lam * pd.Series(gcm_weights['GcmWeight']) @ np.asarray(fund_sims.cov())
        implied_returns = implied_returns + exp_rf
        return_scalar = exp_return / (pd.Series(gcm_weights['GcmWeight']) @ implied_returns)
        implied_returns = pd.DataFrame({'Fund': fund_sims.columns,
                                        'ImpliedReturn': implied_returns * return_scalar,
                                        'GcmFirmwideWeight': gcm_weights['GcmWeight']})
        implied_returns.to_csv('implied_returns.csv')

        tmp = (pd.Series([1 / 65] * 65) @ np.asarray(fund_sims.cov()))
        tmp = pd.DataFrame(tmp, index=fund_sim_cor_mat.index).sort_values(0)
