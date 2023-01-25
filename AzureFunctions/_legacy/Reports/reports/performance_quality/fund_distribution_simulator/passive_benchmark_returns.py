import datetime as dt
import pandas as pd
import scipy
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.utils.date import DatePeriod
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import (
    AggregateFromDaily,
)
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.scenario import Scenario
from pandas import DataFrame
import numpy as np

runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

equity_tickers = ['GDDUWI Index', 'M7EU Index', 'MXCN Index', 'SPTRRLST Index',
                  'SPTRCOND Index', 'SPTRENRS Index', 'SPTRFINL Index', 'SPTRHLTH Index',
                  'SPTRINDU Index', 'SPTRUTIL Index', 'SPXT Index', 'XNDX Index']

def _get_move_index_levels(as_of_date):
    daily_levels = Factor(tickers=["MOVE Index"]).get_dimensions(DatePeriod(start_date=dt.date(2000, 1, 1),
                                                                            end_date=as_of_date))
    daily_levels = daily_levels.pivot_table(index="Date", columns="Ticker", values="PxLast")

    levels = AggregateFromDaily().transform(
        data=daily_levels,
        method="last",
        period=Periodicity.Monthly,
        first_of_day=True
    )
    return levels


def get_monthly_returns(tickers, as_of_date):
    daily_returns = Factor(tickers=[x for x in tickers if x != 'MOVE Index']).get_returns(
        start_date=dt.date(1990, 1, 1),
        end_date=as_of_date,
        fill_na=True,
    )

    returns = AggregateFromDaily().transform(
        data=daily_returns,
        method="geometric",
        period=Periodicity.Monthly,
        first_of_day=True
    )

    if "MOVE Index" in tickers:
        move_levels = _get_move_index_levels(as_of_date=as_of_date)
        returns = returns.merge(move_levels, left_index=True, right_index=True, how='outer')

    return returns


def _roll_returns(monthly_returns, window=36):
    rolling_3y_bmrk = Analytics().compute_trailing_return(ror=monthly_returns.drop(columns={'MOVE Index'}),
                                                          window=window,
                                                          as_of_date=monthly_returns.index.max(),
                                                          method='geometric',
                                                          periodicity=Periodicity.Monthly,
                                                          annualize=True,
                                                          include_history=True)

    if 'MOVE Index' in monthly_returns.columns:
        rolling_move = monthly_returns['MOVE Index'].rolling(window).mean().dropna()
        rolling_3y_bmrk = rolling_3y_bmrk.merge(rolling_move, left_index=True, right_index=True, how='outer')
    return rolling_3y_bmrk


def _normalize_equity_returns(bmrk_return, exp_rf, equity_risk_premia=0.055,
                              equity_ticker='GDDUWI Index', rf_ticker='I00078US Index'):
    erp = equity_risk_premia
    msci_world = equity_ticker
    equity_summary = pd.DataFrame()
    for bmrk in equity_tickers:
        if bmrk == msci_world:
            beta = 1
            exp_rtn = round(erp + exp_rf, 3)
            hist_alpha = 0
        else:
            risk_premia = bmrk_return[[msci_world, rf_ticker, bmrk]].dropna()
            risk_premia[msci_world] = risk_premia[msci_world] - risk_premia[rf_ticker]
            risk_premia[bmrk] = risk_premia[bmrk] - risk_premia[rf_ticker]

            beta = scipy.stats.linregress(x=risk_premia[msci_world], y=risk_premia[bmrk])[0].round(2)
            hist_alpha = scipy.stats.linregress(x=risk_premia[msci_world], y=risk_premia[bmrk])[1].round(3)

            sample_coverage = risk_premia.shape[0] / bmrk_return[[msci_world]].shape[0]
            beta = (sample_coverage * beta + (1 - sample_coverage) * 1).round(2)

            exp_rtn = (beta * erp) + exp_rf
            exp_rtn = round(exp_rtn, 3)

        summary = pd.DataFrame({'bmrk': [bmrk], 'beta': [beta], 'exp_return': [exp_rtn], 'hist_alpha': [hist_alpha]})
        equity_summary = pd.concat([equity_summary, summary], axis=0)

    adj_equity_returns: DataFrame = bmrk_return[equity_tickers]
    for bmrk in equity_tickers:
        historical = adj_equity_returns[bmrk].mean()
        expected = equity_summary.loc[equity_summary['bmrk'] == bmrk, 'exp_return'].squeeze()
        delta = expected - historical
        adj_equity_returns[bmrk] = adj_equity_returns[bmrk] + delta

    msci_world_sharpe = ((adj_equity_returns[msci_world].mean() - exp_rf) / adj_equity_returns[msci_world].std())
    equity_sharpe_lb = msci_world_sharpe * 0.8
    equity_sharpe_ub = msci_world_sharpe * 1.2
    equity_summary = equity_summary.set_index('bmrk')
    for bmrk in equity_tickers:
        adj_return = adj_equity_returns[bmrk].mean()
        lb_return = (equity_sharpe_lb * adj_equity_returns[bmrk].std()) + exp_rf
        ub_return = (equity_sharpe_ub * adj_equity_returns[bmrk].std()) + exp_rf
        if adj_return < lb_return:
            delta = lb_return - adj_return
            adj_equity_returns[bmrk] = adj_equity_returns[bmrk] + delta
        elif adj_return > ub_return:
            delta = adj_return - ub_return
            adj_equity_returns[bmrk] = adj_equity_returns[bmrk] - delta

        exp_return = adj_equity_returns[bmrk].mean()
        exp_vol_1y = adj_equity_returns[bmrk].std() / (1 / 3) ** (1 / 2)
        equity_summary.loc[bmrk, 'exp_return'] = exp_return
        equity_summary.loc[bmrk, 'exp_std_1y'] = exp_vol_1y
        equity_summary.loc[bmrk, 'exp_sharpe'] = (exp_return - exp_rf) /exp_vol_1y

    equity_summary = equity_summary[['beta', 'hist_alpha', 'exp_return', 'exp_std_1y', 'exp_sharpe']]
    equity_summary = equity_summary.sort_values('exp_return', ascending=False)

    return equity_summary, adj_equity_returns


def _normalize_credit_returns(exp_rf, bmrk_return, hy_risk_premia=0.04, hy_ticker='LG30TRUH Index'):
    hy_delta = (hy_risk_premia + exp_rf) - bmrk_return[hy_ticker].mean()
    adj_credit_returns = bmrk_return[hy_ticker] + hy_delta
    return adj_credit_returns


def _normalize_rf_returns(exp_rf, bmrk_return, rf_ticker='I00078US Index'):
    return bmrk_return[rf_ticker] + (exp_rf - bmrk_return[rf_ticker].mean())


def query_historical_benchmark_returns(as_of_date, lookback_years=20, rolling_years=3):
    with Scenario(dao=runner, as_of_date=as_of_date).context():
        non_equity_tickers = ['LG30TRUH Index', 'MOVE Index']
        rf = 'I00078US Index'
        tickers = equity_tickers + non_equity_tickers + [rf]
        if rolling_years == 3:
            bmrk_return = _roll_returns(get_monthly_returns(tickers, as_of_date))
        else:
            raise ValueError("Rolling window not supported. Must use 3 years.")

    bmrk_return = bmrk_return.tail(lookback_years * 12)
    return bmrk_return


def normalize_benchmark_returns(bmrk_return, exp_rf):
    equity_summary, adj_equity_returns = _normalize_equity_returns(bmrk_return=bmrk_return,
                                                                   exp_rf=exp_rf)
    adj_credit_returns = _normalize_credit_returns(bmrk_return=bmrk_return, exp_rf=exp_rf)
    adj_rf_returns = _normalize_rf_returns(bmrk_return=bmrk_return,exp_rf=exp_rf)
    adj_move_levels = bmrk_return['MOVE Index']

    adj_returns = adj_equity_returns.merge(adj_rf_returns, left_index=True, right_index=True)
    adj_returns = adj_returns.merge(adj_credit_returns, left_index=True, right_index=True)
    adj_returns = adj_returns.merge(adj_move_levels, left_index=True, right_index=True)

    assert all(bmrk_return.columns.isin(adj_returns.columns))

    return adj_returns


def summarize_benchmark_returns(rolling_3y_returns, exp_rf):
    ann_ror = rolling_3y_returns.mean()
    ann_3y_vol = rolling_3y_returns.std()
    ann_1y_vol = ann_3y_vol / (np.sqrt(1 / 3))
    ann_sharpe = (ann_ror - exp_rf) / ann_1y_vol
    return_summary = pd.concat([ann_ror, ann_3y_vol, ann_1y_vol, ann_sharpe], axis=1)
    return_summary.columns = ['AnnReturn', 'AnnVol3y', 'AnnVol1y', 'AnnSharpe1y']
    return_summary.loc['MOVE Index', ['AnnVol3y', 'AnnVol1y', 'AnnSharpe1y']] = None
    return_summary = return_summary.sort_values('AnnReturn', ascending=False)
    return_summary = return_summary.round(2)
    return return_summary


if __name__ == "__main__":
    exp_rf = 0.03
    historical_returns = query_historical_benchmark_returns(as_of_date=dt.date(2022, 12, 31),
                                                            lookback_years=20)
    print(summarize_benchmark_returns(rolling_3y_returns=historical_returns, exp_rf=exp_rf))

    adj_returns = normalize_benchmark_returns(bmrk_return=historical_returns, exp_rf=exp_rf)

    print(summarize_benchmark_returns(rolling_3y_returns=adj_returns, exp_rf=exp_rf))

    adj_returns_corr_mat = adj_returns.corr().round(2)
