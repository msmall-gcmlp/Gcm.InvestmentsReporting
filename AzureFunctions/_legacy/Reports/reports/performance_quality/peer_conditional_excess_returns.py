import pandas as pd
import datetime as dt
import os
import scipy.stats
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import AggregateFromDaily
import numpy as np
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario
from gcm.inv.utils.date import DatePeriod


def _collect_input_data(peer_group, start_date, end_date):
    peer_returns = StrategyBenchmark().get_altsoft_peer_constituent_returns(start_date=start_date,
                                                                            end_date=end_date,
                                                                            peer_names=[peer_group],
                                                                            wide=True)
    peer_returns.columns = peer_returns.columns.droplevel(0)

    peer_arb_mapping = pd.read_csv(os.path.dirname(__file__) + "/peer_group_to_arb_mapping.csv")
    passive_bmrk = peer_arb_mapping[peer_arb_mapping['ReportingPeerGroup'] == peer_group]
    passive_bmrk = passive_bmrk['Ticker'].squeeze()

    if peer_group == 'GCM Macro':
        passive_bmrk = "MOVE Index"
        fin_index_returns = Factor(tickers=[passive_bmrk]).get_dimensions(DatePeriod(start_date=start_date,
                                                                                     end_date=end_date))
        fin_index_returns = fin_index_returns.pivot_table(index="Date", columns="Ticker", values="PxLast")
        fin_index_returns = AggregateFromDaily().transform(
            data=fin_index_returns,
            method="last",
            period=Periodicity.Monthly,
            first_of_day=True
        )
    else:
        fin_index_returns = Factor(tickers=[passive_bmrk]).get_returns(start_date=start_date,
                                                                       end_date=end_date,
                                                                       fill_na=True)
        fin_index_returns = AggregateFromDaily().transform(
            data=fin_index_returns,
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True
        )

    return peer_returns, fin_index_returns


def _compute_rolling_excess_metrics(peer_returns, fin_index_returns, percentiles=[25, 50, 75], window=36):
    peer_bmrk_ror = peer_returns.merge(fin_index_returns, how='left', left_index=True, right_index=True)
    passive_bmrk = fin_index_returns.columns[0]
    betas = []
    excess_returns = pd.DataFrame()

    if passive_bmrk != 'MOVE Index':
        for fund in peer_bmrk_ror.columns[:-1]:
            fund_returns = peer_bmrk_ror[[passive_bmrk, fund]].dropna()

            if fund_returns.shape[0] > 0:
                beta = scipy.stats.linregress(x=fund_returns[passive_bmrk], y=fund_returns[fund])[0]
                beta = round(max(min(beta, 1.5), 0.1), 1)
                betas.append(beta)

                beta_adj_index = peer_bmrk_ror[passive_bmrk] * beta
                excess_return = peer_bmrk_ror[fund] - beta_adj_index
                excess_return = excess_return.to_frame(fund)
                excess_returns = pd.concat([excess_returns, excess_return], axis=1)
    else:
        excess_returns = peer_returns.copy()

    rolling_excess_returns = Analytics().compute_trailing_return(ror=excess_returns,
                                                                 window=window,
                                                                 as_of_date=excess_returns.index.max(),
                                                                 method='geometric',
                                                                 periodicity=Periodicity.Monthly,
                                                                 annualize=True,
                                                                 include_history=True)

    # np.nanpercentile(np.array(rolling_3y_excess_returns.values.tolist()), 75)

    excess_ptiles = pd.DataFrame()
    for ptile in percentiles:
        excess_ptile = rolling_excess_returns.apply(lambda x: np.nanpercentile(x, q=ptile), axis=1)
        excess_ptile = excess_ptile.to_frame(ptile)
        excess_ptiles = pd.concat([excess_ptiles, excess_ptile], axis=1)

    # excess_ptiles.mean()

    return excess_ptiles


def _compute_market_scenarios(fin_index_returns, market_ptiles, window=36):
    passive_bmrk = fin_index_returns.columns[0]
    if passive_bmrk == 'MOVE Index':
        rolling_3y_bmrk = fin_index_returns.rolling(window).mean().dropna()
    else:
        rolling_3y_bmrk = Analytics().compute_trailing_return(ror=fin_index_returns,
                                                              window=window,
                                                              as_of_date=fin_index_returns.index.max(),
                                                              method='geometric',
                                                              periodicity=Periodicity.Monthly,
                                                              annualize=True,
                                                              include_history=True)

    # sum(mkt_probs) == 1
    # spx_percentiles = 100 * np.cumsum(mkt_probs)
    # spx_percentiles = spx_percentiles[:-1]

    spx_ror_cutoffs = np.percentile(rolling_3y_bmrk, market_ptiles)
    spx_ror_cutoffs = [-np.inf] + spx_ror_cutoffs.round(3).tolist() + [np.inf]

    rolling_3y_bmrk['MarketScenario'] = pd.cut(rolling_3y_bmrk.iloc[:, 0],
                                               bins=spx_ror_cutoffs,
                                               labels=['< 10th', '10th - 25th', '25th - 50th',
                                                       '50th - 75th', '75th - 90th', '> 90th'])

    return rolling_3y_bmrk


def _summarize_strategy_excess(market_scenarios, excess_ptiles):
    conditional_ptiles = market_scenarios.merge(excess_ptiles, how='left', left_index=True, right_index=True)
    conditional_ptile_summary = conditional_ptiles.groupby('MarketScenario').mean()
    conditional_ptile_summary = conditional_ptile_summary.round(2)
    return conditional_ptile_summary


def generate_peer_conditional_excess_returns(peer_group):
    start_date = dt.date(2000, 1, 1)
    end_date = Scenario.get_attribute("as_of_date")
    peer_returns, fin_index_returns = _collect_input_data(peer_group=peer_group,
                                                          start_date=start_date,
                                                          end_date=end_date)
    # note Market Percentiles and Peer Percentiles do not have to be the same. Just happen to be here.

    excess_ptiles = _compute_rolling_excess_metrics(peer_returns=peer_returns,
                                                    fin_index_returns=fin_index_returns,
                                                    percentiles=[10, 25, 50, 75, 90])

    market_scenarios = _compute_market_scenarios(fin_index_returns=fin_index_returns,
                                                 market_ptiles=[10, 25, 50, 75, 90])

    conditional_ptile_summary = _summarize_strategy_excess(market_scenarios=market_scenarios,
                                                           excess_ptiles=excess_ptiles)

    return market_scenarios, conditional_ptile_summary


if __name__ == "__main__":
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

    with Scenario(runner=runner, as_of_date=dt.date(2022, 10, 31)).context():
        market_scenarios, conditional_ptile_summary = \
            generate_peer_conditional_excess_returns(peer_group='GCM Macro')
        print(market_scenarios)
