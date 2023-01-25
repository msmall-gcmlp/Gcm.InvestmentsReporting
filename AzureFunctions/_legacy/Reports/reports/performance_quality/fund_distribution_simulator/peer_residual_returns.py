import datetime as dt
from functools import cached_property

import pandas as pd

from _legacy.Reports.reports.performance_quality.fund_distribution_simulator.passive_benchmark_returns import \
    get_monthly_returns, query_historical_benchmark_returns, summarize_benchmark_returns
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


class PeerResidualReturns(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    @cached_property
    def peer_bmrk_mapping(self):
        peer_arb_mapping = pd.read_csv(os.path.dirname(__file__) + "/../peer_group_to_arb_mapping.csv")
        peer_arb_mapping.loc[peer_arb_mapping['ReportingPeerGroup'] == 'GCM Macro', 'Ticker'] = 'MOVE Index'
        return peer_arb_mapping

    @cached_property
    def gcm_cap_weighted_returns(self):
        returns = pd.read_csv(os.path.dirname(__file__) + "/gcm_cap_weighted_rors.csv")
        returns['Date'] = pd.to_datetime(returns['Date'])
        returns = returns.set_index(['Date'])
        return returns

    @cached_property
    def benchmark_returns(self):
        return get_monthly_returns(tickers=self.peer_bmrk_mapping['Ticker'].unique().tolist(),
                                   as_of_date=self._as_of_date)

    @cached_property
    def _ehi_benchmarks(self):
        return self.peer_bmrk_mapping['EhiBenchmark'].unique().tolist()

    @cached_property
    def ehi_benchmark_returns(self):
        return StrategyBenchmark().get_eurekahedge_returns(
            start_date=dt.date(2008, 1, 1), end_date=self._as_of_date, benchmarks_names=self._ehi_benchmarks
        )

    @cached_property
    def _ehi_constituent_returns(self):
        returns = StrategyBenchmark().get_eurekahedge_constituent_returns(start_date=dt.date(2000, 1, 1),
                                                                          end_date=self._as_of_date,
                                                                          benchmarks_names=self._ehi_benchmarks)
        return returns

    def query_ehi_constituent_returns(self, benchmark_name, min_peer_coverage=0.3):
        returns = self._ehi_constituent_returns
        returns = returns[benchmark_name]
        returns = self._filter_peer_returns(returns=returns, min_peer_coverage=min_peer_coverage)
        return returns

    @staticmethod
    def _filter_peer_returns(returns, min_peer_coverage):
        not_na = returns.iloc[:, 2:].notna().sum(axis=1) / (returns.shape[1])
        min_date = not_na[not_na > min_peer_coverage].index[0].strftime('%Y-%m-%d')
        returns = returns.loc[min_date:]
        return returns

    def query_peer_returns(self, peer, min_peer_coverage=0.3):
        returns = PeerGroup().get_constituent_returns(start_date=dt.date(2000, 1, 1),
                                                      end_date=self._as_of_date,
                                                      peer_groups=peer)
        returns.columns = returns.columns.droplevel(0)
        returns = returns.astype(float)
        returns = self._filter_peer_returns(returns=returns, min_peer_coverage=min_peer_coverage)
        return returns

    def get_peer_benchmark(self, peer):
        return self.peer_bmrk_mapping[self.peer_bmrk_mapping['ReportingPeerGroup'] == peer]['Ticker'].squeeze()

    def get_peer_gcm_substrat(self, peer):
        return self.peer_bmrk_mapping[self.peer_bmrk_mapping['ReportingPeerGroup'] == peer]['BenchmarkSubstrategy'].squeeze()

    def get_peer_ehi_benchmark(self, peer):
        return self.peer_bmrk_mapping[self.peer_bmrk_mapping['ReportingPeerGroup'] == peer]['EhiBenchmark'].squeeze()

    @staticmethod
    def calculate_ptile_bmrks(constituent_returns, ptiles=[25, 50, 75]):
        bmrks = pd.DataFrame(index=constituent_returns.index)
        for ptile in ptiles:
            bmrk = constituent_returns.iloc[:, 2:].apply(lambda x: np.nanpercentile(x, q=ptile), axis=1)
            bmrk = bmrk.to_frame('Peer' + str(ptile))
            bmrks = bmrks.merge(bmrk, left_index=True, right_index=True)
        return bmrks

    def generate_historical_peer_summary(self, peer_excess, bmrk_returns, peer_name):
        returns = peer_excess.merge(bmrk_returns, left_index=True, right_index=True, how='outer')
        bmrk_name = bmrk_returns.columns[0]
        peer = peer_name
        fit = scipy.stats.linregress(x=returns[bmrk_name], y=returns['Peer50'])

        min_date = returns.index.min().strftime('%b %Y')

        corr_25th = returns.corr().loc[bmrk_name, 'Peer25']
        corr_50th = returns.corr().loc[bmrk_name, 'Peer50']
        corr_75th = returns.corr().loc[bmrk_name, 'Peer75']

        peer_bmrk_vol = returns.std()['Peer50']
        bmrk_vol = returns.std()[bmrk_name]

        historical_residuals = \
            self.calculate_residuals(x=bmrk_returns[bmrk_name],
                                     y=peer_excess['Peer50'],
                                     beta=fit[0],
                                     alpha=fit[1])

        resid_vol = historical_residuals.std()

        summary = pd.DataFrame({'MinDate': [min_date],
                                'Corr_25th': [corr_25th],
                                'Corr': [corr_50th],
                                'PeerCorr_75th': [corr_75th],
                                'Beta': [fit[0]],
                                'Alpha': [fit[1]],
                                'Vol': [peer_bmrk_vol],
                                'ResidVol': [resid_vol],
                                'VolOverBmrkVol': [peer_bmrk_vol / bmrk_vol],
                                'ResidVolOverVol': [resid_vol / peer_bmrk_vol],
                                'BmrkVol': [bmrk_vol],
                                'BmrkName': [bmrk_name]},
                               index=pd.MultiIndex.from_tuples([(peer, 'Peer')], names=["PeerGroup", "BmrkType"]))

        return summary, historical_residuals

    def generate_historical_ehi_constituent_summary(self, peer_excess, bmrk_returns, peer_name):
        returns = peer_excess.merge(bmrk_returns, left_index=True, right_index=True, how='outer')
        bmrk_name = bmrk_returns.columns[0]
        peer = returns.columns[0]
        fit = scipy.stats.linregress(x=returns[bmrk_name], y=returns[peer])

        min_date = returns.index.min().strftime('%b %Y')

        corr = returns.corr().loc[bmrk_name, peer]

        peer_bmrk_vol = returns.std()[peer]
        bmrk_vol = returns.std()[bmrk_name]

        historical_residuals = \
            self.calculate_residuals(x=bmrk_returns[bmrk_name],
                                     y=peer_excess[peer],
                                     beta=fit[0],
                                     alpha=fit[1])

        resid_vol = historical_residuals.std()

        summary = pd.DataFrame({'MinDate': [min_date],
                                'Corr': [corr],
                                'Beta': [fit[0]],
                                'Alpha': [fit[1]],
                                'Vol': [peer_bmrk_vol],
                                'ResidVol': [resid_vol],
                                'VolOverBmrkVol': [peer_bmrk_vol / bmrk_vol],
                                'ResidVolOverVol': [resid_vol / peer_bmrk_vol],
                                'BmrkVol': [bmrk_vol],
                                'BmrkName': [bmrk_name]},
                               index=pd.MultiIndex.from_tuples([(peer_name, 'EhiTop')], names=["PeerGroup", "BmrkType"]))
        return summary, historical_residuals

    def generate_historical_bmrk_summary(self, peer_name, bmrk_type):
        returns = self.calculate_benchmark_excess(peer_name, bmrk_type=bmrk_type)
        bmrk_name = returns.columns[1]
        strat_name = returns.columns[0]
        fit = scipy.stats.linregress(x=returns[bmrk_name], y=returns[strat_name])

        min_date = returns.index.min().strftime('%b %Y')
        corr = returns.corr().loc[bmrk_name, strat_name]
        peer_bmrk_vol = returns.std()[strat_name]
        bmrk_vol = returns.std()[bmrk_name]

        historical_residuals = \
            self.calculate_residuals(x=returns[bmrk_name],
                                     y=returns[strat_name],
                                     beta=fit[0],
                                     alpha=fit[1])

        resid_vol = historical_residuals.std()

        summary = pd.DataFrame({'MinDate': [min_date],
                                'Corr': [corr],
                                'Beta': [fit[0]],
                                'Alpha': [fit[1]],
                                'Vol': [peer_bmrk_vol],
                                'ResidVol': [resid_vol],
                                'VolOverBmrkVol': [peer_bmrk_vol / bmrk_vol],
                                'ResidVolOverVol': [resid_vol / peer_bmrk_vol],
                                'BmrkVol': [bmrk_vol],
                                'BmrkName': [bmrk_name],
                                'Strategy': [strat_name]},
                               index=pd.MultiIndex.from_tuples([(peer, bmrk_type)], names=["PeerGroup", "BmrkType"]))
        return summary, historical_residuals

    @staticmethod
    def calculate_residuals(x, y, beta, alpha):
        return y - (beta * x + alpha)

    def calculate_benchmark_excess(self, peer_name, bmrk_type='gcm'):
        if bmrk_type == 'gcm':
            hf_returns = self.gcm_cap_weighted_returns
            substrat = self.get_peer_gcm_substrat(peer=peer_name)

        elif bmrk_type == 'ehi':
            hf_returns = self.ehi_benchmark_returns
            substrat = self.get_peer_ehi_benchmark(peer=peer_name)

        monthly_returns = hf_returns.merge(self.benchmark_returns, left_index=True, right_index=True,  how='left')

        bmrk = self.get_peer_benchmark(peer=peer_name)

        tmp = monthly_returns[[substrat, bmrk]].dropna()
        fit = scipy.stats.linregress(x=tmp[bmrk], y=tmp[substrat])
        cw_excess = tmp[substrat] - (fit[0] * tmp[bmrk])
        excess_and_bmrk = cw_excess.to_frame(substrat).merge(tmp[[bmrk]], left_index=True, right_index=True)

        rolling_returns = Analytics().compute_trailing_return(ror=excess_and_bmrk,
                                                              window=36,
                                                              as_of_date=self._as_of_date,
                                                              method='geometric',
                                                              periodicity=Periodicity.Monthly,
                                                              annualize=True,
                                                              include_history=True)
        return rolling_returns

    def calculate_median_constituent_vol(self, constituent_returns, window=36):
        rolling_returns = Analytics().compute_trailing_return(ror=constituent_returns,
                                                              window=window,
                                                              as_of_date=self._as_of_date,
                                                              method='geometric',
                                                              periodicity=Periodicity.Monthly,
                                                              annualize=True,
                                                              include_history=True)
        median_1y_vol = (rolling_returns.std() / np.sqrt(12 / 36)).median()
        return median_1y_vol

    @staticmethod
    def calculate_median_constituent_beta(constituent_returns, benchmark_returns):
        hf_returns = constituent_returns.mean(axis=1).to_frame('Hf')
        returns = hf_returns.merge(benchmark_returns, left_index=True, right_index=True).dropna()
        fit = scipy.stats.linregress(x=returns[benchmark_returns.columns[0]], y=returns['Hf'])
        beta = fit[0]
        return beta

    def generate_correlation_summaries(self, peer_residuals):
        arb_returns = query_historical_benchmark_returns(as_of_date=self._as_of_date, lookback_years=20)
        corr_summaries = pd.DataFrame()
        peer_arb_mapping = self.peer_bmrk_mapping
        for peer in peer_residuals.columns.get_level_values(0).unique().tolist():
            residuals = peer_residuals[[peer]]
            corrs = pd.concat([residuals, arb_returns], axis=1).corr()
            avg_arb_corr = corrs.iloc[4:, 0:4].mean().to_frame('AvgArbCorr')
            max_arb_corr = corrs.iloc[4:, 0:4].abs().max().to_frame('MaxArbCorr')
            max_arb_corr_pair = corrs.iloc[4:, 0:4].abs().idxmax().to_frame('MaxArbCorrPair').apply(
                lambda x: x.replace(' Index', '', regex=True))

            arb_corr_pairs = pd.concat([max_arb_corr_pair, max_arb_corr], axis=1)
            arb_corr_pairs = arb_corr_pairs['MaxArbCorrPair'] + '\n (' + arb_corr_pairs['MaxArbCorr'].round(1).astype(
                str) + ')'
            arb_corr_pairs = arb_corr_pairs.to_frame('ArbCorrPairs')
            arb_corr_summary = pd.concat([avg_arb_corr, arb_corr_pairs], axis=1)

            peer_corrs = peer_residuals.corr()[[peer]]
            inter_peer_corr = peer_corrs[peer_corrs.index.get_level_values(0) != peer]
            avg_inter_peer_corr = inter_peer_corr.mean().to_frame('AvgInterPeerCorr')

            inter_peer_corr = inter_peer_corr.groupby('PeerGroup').mean()
            min_peer_corr = inter_peer_corr.min().to_frame('MinPeerCorr')
            max_peer_corr = inter_peer_corr.max().to_frame('MaxPeerCorr')

            # inter_peer_corr.index = inter_peer_corr.index.droplevel(1)
            mapping = peer_arb_mapping[['ReportingPeerGroup', 'PeerGroupShortName']]
            min_peer_corr_pair = inter_peer_corr.idxmin().to_frame('MinPeerCorrPair')
            max_peer_corr_pair = inter_peer_corr.idxmin().to_frame('MaxPeerCorrPair')
            min_peer_corr_pair = min_peer_corr_pair.replace(
                dict(zip(mapping.ReportingPeerGroup, mapping.PeerGroupShortName)))
            max_peer_corr_pair = max_peer_corr_pair.replace(
                dict(zip(mapping.ReportingPeerGroup, mapping.PeerGroupShortName)))

            min_peer_corr_pairs = pd.concat([min_peer_corr_pair, min_peer_corr], axis=1)
            max_peer_corr_pairs = pd.concat([max_peer_corr_pair, max_peer_corr], axis=1)
            min_peer_corr_pairs = min_peer_corr_pairs['MinPeerCorrPair'] + '\n (' + min_peer_corr_pairs[
                'MinPeerCorr'].round(1).astype(str) + ')'
            max_peer_corr_pairs = max_peer_corr_pairs['MaxPeerCorrPair'] + '\n (' + max_peer_corr_pairs[
                'MaxPeerCorr'].round(1).astype(str) + ')'

            min_peer_corr_pairs = min_peer_corr_pairs.to_frame('MinInterPeerCorrPairs')
            max_peer_corr_pairs = max_peer_corr_pairs.to_frame('MaxInterPeerCorrPairs')
            peer_corr_summary = pd.concat([avg_inter_peer_corr, min_peer_corr_pairs, max_peer_corr_pairs], axis=1)
            corr_summary = pd.concat([arb_corr_summary, peer_corr_summary], axis=1)
            corr_summaries = pd.concat([corr_summaries, corr_summary])
        return corr_summaries

    @staticmethod
    def generate_excel_inputs(historical_peer_summary, col_order):
        tmp = historical_peer_summary[['BmrkName', 'Strategy']].reset_index()
        arb = tmp[tmp['level_1'] == 'ehi'][['level_0', 'BmrkName']].set_index('level_0').rename(
            columns={'BmrkName': 'ARB'})
        ehi = tmp[tmp['level_1'] == 'ehi'][['level_0', 'Strategy']].set_index('level_0').rename(
            columns={'Strategy': 'EHI'})
        gcm = tmp[tmp['level_1'] == 'gcm'][['level_0', 'Strategy']].set_index('level_0').rename(
            columns={'Strategy': 'GCM'})
        benchmark_assignments = pd.concat([arb, ehi, gcm], axis=1).loc[col_order].T

        def _format(data, field, row_order=['Peer', 'EhiTop', 'ehi', 'gcm']):
            summary = data[field].reset_index()
            summary = summary.pivot(index='level_1', columns='level_0')

            summary.columns = summary.columns.droplevel(0)
            summary = summary.loc[row_order, col_order]
            return summary.loc[row_order, col_order]

        excel_data = {
            "historical_total_vol": _format(historical_peer_summary, field='ItdTotalVol', row_order=['Peer', 'ehi']),
            "historical_beta_arb": _format(historical_peer_summary, field='ItdBetaVsBmrk', row_order=['Peer', 'ehi']),
            "min_return_date": _format(historical_peer_summary, field='MinDate'),
            "historical_excess_return": _format(historical_peer_summary, field='Alpha'),
            "historical_excess_vol": _format(historical_peer_summary, field='Vol'),
            "historical_excess_arb_vol_ratio": _format(historical_peer_summary, field='VolOverBmrkVol'),
            "historical_residual_vol": _format(historical_peer_summary, field='ResidVol'),
            "historical_excess_corr_vs_arb": _format(historical_peer_summary, field='Corr'),
            "historical_5y_avg_corr_to_other_bmrks": _format(historical_peer_summary, field='IntraPeerBmrkCorr5Y'),
            "historical_itd_avg_corr_to_other_bmrks": _format(historical_peer_summary, field='IntraPeerBmrkCorrItd'),
            "historical_excess_corr_vs_all_arbs": _format(historical_peer_summary, field='AvgArbCorr'),
            "historical_excess_corr_arb_pairs": _format(historical_peer_summary, field='ArbCorrPairs'),
            "historical_excess_corr_vs_all_peers": _format(historical_peer_summary, field='AvgInterPeerCorr'),
            "historical_excess_min_corr_peer_pairs": _format(historical_peer_summary, field='MinInterPeerCorrPairs'),
            "historical_excess_max_corr_peer_pairs": _format(historical_peer_summary, field='MaxInterPeerCorrPairs'),
            "benchmark_assignments": benchmark_assignments
        }
        return excel_data

    def generate_excel_report(self, col_order, historical_peer_summary):
        col_order = self.peer_bmrk_mapping.set_index('PeerGroupShortName').loc[col_order]['ReportingPeerGroup']
        col_order = col_order.squeeze().tolist()
        excel_data = self.generate_excel_inputs(historical_peer_summary, col_order=col_order)

        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=excel_data,
                template="ARS_Fund_Distribution_Model_Diagnostics_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name='ARS',
                entity_display_name='ARS',
                entity_ids='',
                report_name="ARS Fund Distribution Model Diagnostics",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                # aggregate_intervals=AggregateInterval.MTD,
                output_dir="cleansed/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake,
            )

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
        peer_groups = [
            'GCM Generalist Long/Short Equity',
            'GCM Multi-PM',
            # 'GCM Macro',
            # 'GCM DS Multi-Strategy',
            # 'GCM Quant'
            # 'GCM Relative Value',
            # 'GCM Multi-Strategy',
            # 'GCM Fundamental Credit',
            # 'GCM Long/Short Credit',
            # 'GCM Structured Credit',
            # 'GCM Consumer',
            # 'GCM Healthcare',
            # 'GCM TMT',
            # 'GCM Energy',
            # 'GCM Financials',
            # 'GCM China',
            # 'GCM Asia Equity',
            # 'GCM Europe Credit',
            # 'GCM Europe Equity',
            # 'GCM Cross Cap'
        ]

        peer_ptiles = [25, 50, 75]

        peer_residuals = PeerResidualReturns()
        benchmark_returns = peer_residuals.benchmark_returns
        historical_peer_summary = []
        historical_peer_residuals = pd.DataFrame()

        for peer in peer_groups:
            print(peer)
            bmrk_name = peer_residuals.get_peer_benchmark(peer=peer)
            bmrk_monthly_returns = benchmark_returns[[bmrk_name]]

            constituent_monthly_returns = peer_residuals.query_peer_returns(peer=peer, min_peer_coverage=0.3)
            ehi_bmrk = peer_residuals.get_peer_ehi_benchmark(peer)
            ehi_constituent_monthly_returns = peer_residuals.query_ehi_constituent_returns(benchmark_name=ehi_bmrk, min_peer_coverage=0.3)

            constituent_excess = calculate_rolling_excess_returns(peer_returns=constituent_monthly_returns,
                                                                  benchmark_returns=bmrk_monthly_returns)

            ehi_constituent_excess = calculate_rolling_excess_returns(peer_returns=ehi_constituent_monthly_returns,
                                                                      benchmark_returns=bmrk_monthly_returns)

            peer_bmrk_excess = peer_residuals.calculate_ptile_bmrks(constituent_returns=constituent_excess,
                                                                    ptiles=peer_ptiles)

            ehi_top_qtile_excess = peer_residuals.calculate_ptile_bmrks(constituent_returns=ehi_constituent_excess,
                                                                        ptiles=[63])

            peer_summary, peer_res = peer_residuals.generate_historical_peer_summary(peer_excess=peer_bmrk_excess,
                                                                                     bmrk_returns=constituent_excess.iloc[:, 0].to_frame(),
                                                                                     peer_name=peer)
            peer_summary.loc[:, 'ItdTotalVol'] = peer_residuals.calculate_median_constituent_vol(
                constituent_returns=constituent_monthly_returns)
            peer_summary.loc[:, 'ItdBetaVsBmrk'] = peer_residuals.calculate_median_constituent_beta(
                constituent_returns=constituent_monthly_returns, benchmark_returns=bmrk_monthly_returns)

            ehi_top_qtile_summary, ehi_top_qtile_res = \
                peer_residuals.generate_historical_ehi_constituent_summary(peer_excess=ehi_top_qtile_excess,
                                                                           bmrk_returns=ehi_constituent_excess.iloc[:, 0].to_frame(),
                                                                           peer_name=peer)

            gcm_summary, gcm_res = peer_residuals.generate_historical_bmrk_summary(peer_name=peer, bmrk_type='gcm')
            ehi_summary, ehi_res = peer_residuals.generate_historical_bmrk_summary(peer_name=peer, bmrk_type='ehi')

            ehi_summary.loc[:, 'ItdTotalVol'] = peer_residuals.calculate_median_constituent_vol(
                constituent_returns=ehi_constituent_monthly_returns)
            ehi_summary.loc[:, 'ItdBetaVsBmrk'] = peer_residuals.calculate_median_constituent_beta(
                constituent_returns=ehi_constituent_monthly_returns, benchmark_returns=bmrk_monthly_returns)

            summary = pd.concat([peer_summary, gcm_summary, ehi_summary, ehi_top_qtile_summary], axis=0)

            peer_res = peer_res.to_frame()
            peer_res.columns = pd.MultiIndex.from_tuples([(peer, 'Peer')], names=["PeerGroup", "BmrkType"])

            gcm_res = gcm_res.to_frame()
            gcm_res.columns = pd.MultiIndex.from_tuples([(peer, 'gcm')], names=["PeerGroup", "BmrkType"])

            ehi_res = ehi_res.to_frame()
            ehi_res.columns = pd.MultiIndex.from_tuples([(peer, 'ehi')], names=["PeerGroup", "BmrkType"])

            ehi_top_qtile_res = ehi_top_qtile_res.to_frame()
            ehi_top_qtile_res.columns = pd.MultiIndex.from_tuples([(peer, 'EhiTop')], names=["PeerGroup", "BmrkType"])

            residuals = pd.concat([peer_res, gcm_res, ehi_res, ehi_top_qtile_res], axis=1)

            avg_corr_5 = ((residuals.tail(60).corr().sum() - 1) / (residuals.shape[1] - 1)).to_frame('IntraPeerBmrkCorr5Y')
            avg_corr_itd = ((residuals.corr().sum() - 1) / (residuals.shape[1] - 1)).to_frame('IntraPeerBmrkCorrItd')
            summary = pd.concat([summary, avg_corr_5, avg_corr_itd], axis=1)

            historical_peer_summary.append(summary)
            historical_peer_residuals = pd.concat([historical_peer_residuals, residuals], axis=1)
            # fund_summary = pd.DataFrame(index=rolling_excess_returns.columns[2:])
            # for fund in rolling_excess_returns.columns[2:]:
            #     fund_and_median_peer_excess = median_peer_excess.merge(rolling_excess_returns[[fund]], left_index=True, right_index=True)
            #     fund_and_median_peer_excess = fund_and_median_peer_excess.dropna()
            #
            #     if fund_and_median_peer_excess.shape[0] > 0:
            #         fit = scipy.stats.linregress(x=fund_and_median_peer_excess['PeerBmrk'], y=fund_and_median_peer_excess[fund])
            #         fund_summary.loc[fund, 'Beta'] = fit[0]
            #         fund_summary.loc[fund, 'Alpha'] = fit[1]
            #
            #         fund_residuals = fund_and_median_peer_excess[fund] - (fit[0] * fund_and_median_peer_excess['PeerBmrk'] + fit[1])
            #         fund_summary.loc[fund, 'ResidVol'] = fund_residuals.std()
            #         fund_summary.loc[fund, 'ExcessVol'] = fund_and_median_peer_excess[fund].std()
            #         fund_summary.loc[fund, 'CorrToPeerMedian'] = fund_and_median_peer_excess.corr().iloc[0, 1]
            # fund_summaries[peer] = fund_summary

        historical_peer_summary = pd.concat(historical_peer_summary, axis=0)
        corr_summaries = peer_residuals.generate_correlation_summaries(peer_residuals=historical_peer_residuals)
        historical_peer_summary = pd.concat([historical_peer_summary, corr_summaries], axis=1)

        col_order = ['Generalist L/S Eqty',
                     'Multi-PM',
                     # 'Macro',
                     # 'Div Multi-Strat',
                     # 'Quant',
                     # 'Relative Value',
                     # 'Cross Cap',
                     # 'Fdmtl Credit',
                     # 'Structured Credit',
                     # 'L/S Credit',
                     # 'Europe Credit',
                     # 'Consumer',
                     # 'Energy',
                     # 'Financials',
                     # 'Healthcare',
                     # 'TMT',
                     # 'Asia Equity',
                     # 'China',
                     # 'Europe Eqty'
                     ]

        peer_residuals.generate_excel_report(col_order=col_order, historical_peer_summary=historical_peer_summary)

        # historical_peer_summary.to_csv('historical_peer_summary.csv')
        # historical_peer_residuals = historical_peer_residuals.dropna()
        # cor_mat = historical_peer_residuals.corr()  # check correlation of comps (i.e. peer vs. cw vs. eh)
        # cor_mat.to_csv('resid_cor_mat.csv')
        #
        # historical_peer_residuals['GCM Generalist Long/Short Equity'].corr()
        #
        #
        #
        #
        # historical_peer_summary[['PeerAlpha', 'gcmAlpha', 'ehiAlpha', 'EhiTopAlpha']]
        # historical_peer_summary[['PeerCorr_50th', 'gcmCorr', 'ehiCorr', 'EhiTopCorr']]
        # historical_peer_summary[['PeerVolOverBmrkVol', 'gcmVolOverBmrkVol', 'ehiVolOverBmrkVol', 'EhiTopVolOverBmrkVol']]
        #
        # print(fund_summaries['GCM TMT'].loc[['Skye', 'SRS Partners', 'D1 Capital', 'Tiger Global']])
        # print(fund_summaries['GCM Generalist Long/Short Equity'].loc[['D1 Capital', 'Tiger Global', 'Skye']])
        # median_peer_excess_res_cor_mat = median_peer_excess_residuals.corr()
        # median_peer_excess_res_cor_mat.median().sort_values(ascending=False)
        # passive_benchmarks_cor_mat = passive_benchmarks.corr()
        # combined_cor_mat = passive_benchmarks.merge(median_peer_excess_residuals, left_index=True, right_index=True).corr()
        # combined_cor_mat = combined_cor_mat.round(2)
        #
        # confirm close to 0 as we'll be assume excesses are uncorrelated to passive bmrks
        # combined_cor_mat.loc[median_peer_excess_residuals.columns, passive_benchmarks.columns].median().median()
        #
        # confirm not close to 0
        # combined_cor_mat.loc[median_peer_excess_residuals.columns, median_peer_excess_residuals.columns].median().median()
        # summary['alpha'] / summary['ResidVol']
        # tmp = summary.sort_values('CorrToBmrk')
        # combined_cor_mat.loc[['GCM Multi-PM', 'GCM TMT', 'XNDX Index', 'GDDUWI Index'], ['GCM Multi-PM', 'GCM TMT', 'XNDX Index', 'GDDUWI Index']]
