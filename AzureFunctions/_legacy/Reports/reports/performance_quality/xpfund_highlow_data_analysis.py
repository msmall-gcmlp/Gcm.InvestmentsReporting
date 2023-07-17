import datetime as dt
from functools import reduce
from typing import List
import numpy as np
import pandas as pd
import scipy
from gcm.inv.scenario import Scenario
from scipy.stats import spearmanr, percentileofscore
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.dataprovider.portfolio import Portfolio


def _clean_firmwide_xpfund_data(df):
    df = df.fillna(value=np.nan)
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df[df['FirmwideAllocation'].notna()]
    df.FirmwideAllocation = df.FirmwideAllocation.round()
    df['FirmwideAllocation'].replace(0.0, np.nan, inplace=True)
    df = df.dropna(subset=['FirmwideAllocation'])
    return df


def _3y_arb_xs_analysis(df):

    df['ITD_tmp']=pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', 'ITD')"])
    df['ITD_ptiles_default'] = pd.DataFrame(df.ITD_tmp).rank(numeric_only=True, pct = True)
    
    df['default_3y'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', '3Y')"])
    
    df['3y_ptiles'] = pd.DataFrame(df.default_3y).rank(numeric_only=True, pct = True)
    df['3y_ptiles']=df['3y_ptiles'].fillna(df['ITD_ptiles_default'])

    df = df.sort_values(by='3y_ptiles').reset_index()

    return df

def _arb_xs_emm_percentiles(df: pd.DataFrame,
                            period: str
                            ) -> pd.DataFrame:

    tmp_col =['copy_'+period]
    df[tmp_col] = pd.DataFrame(df[f"('AbsoluteReturnBenchmarkExcess', '{period}')"])
    df[period+'_ptiles'] = df[tmp_col].rank(numeric_only=True, pct=True)
    df = df.drop(tmp_col, axis=1)
    return df


def _fund_return_peer_percentiles(df):
    df.rename(columns={
        "('Peer1Ptile', 'TTM')": 'TTM_peer_ptiles',
        "('Peer1Ptile', '3Y')": '3Y_peer_ptiles',
        "('Peer1Ptile', '5Y')": '5Y_peer_ptiles',
        "('Peer1Ptile', '10Y')": '10Y_peer_ptiles'
    }, inplace=True)

    return df

def _net_exposure_clean(df):
    df['net_exp_adj_3y']=df["""('3Y', "('Equities', 'NetNotional')")"""].fillna(0)+0.35*df["""('3Y', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_5y']=df["""('5Y', "('Equities', 'NetNotional')")"""].fillna(0)+0.35*df["""('5Y', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_latest']=df["""('Latest', "('Equities', 'NetNotional')")"""].fillna(0)+0.35*df["""('Latest', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_3y'].replace(0, np.nan, inplace=True)
    df['net_exp_adj_5y'].replace(0, np.nan, inplace=True)
    df['net_exp_adj_latest'].replace(0, np.nan, inplace=True)
    return df

# def _net_exp_adj_3y(df):
#     # df["""('3Y', "('Equities', 'NetNotional')")"""]=df["""('3Y', "('Equities', 'NetNotional')")"""].fillna(0)
#     # df["""('3Y', "('Credit', 'NetNotional')")"""]=df["""('3Y', "('Credit', 'NetNotional')")"""].fillna(0)
#     # df['net_exp_adj_3y']=df["""('3Y', "('Equities', 'NetNotional')")"""]+0.35*df["""('3Y', "('Credit', 'NetNotional')")"""]
#     df['net_exp_adj_3y'].replace(0, np.nan, inplace=True)
#     return df

# def _net_exp_adj_5y(df):
#     # df["""('5Y', "('Equities', 'NetNotional')")"""]=df["""('5Y', "('Equities', 'NetNotional')")"""].fillna(0)
#     # df["""('5Y', "('Credit', 'NetNotional')")"""]=df["""('5Y', "('Credit', 'NetNotional')")"""].fillna(0)
#     # df['net_exp_adj_5y']=df["""('5Y', "('Equities', 'NetNotional')")"""]+0.35*df["""('5Y', "('Credit', 'NetNotional')")"""]
#     df['net_exp_adj_5y'].replace(0, np.nan, inplace=True)
#     return df

# def _net_exp_adj_latest(df):
#     # df["""('Latest', "('Equities', 'NetNotional')")"""]=df["""('Latest', "('Equities', 'NetNotional')")"""].fillna(0)
#     # df["""('Latest', "('Credit', 'NetNotional')")"""]=df["""('Latest', "('Credit', 'NetNotional')")"""].fillna(0)
#     # df['net_exp_adj_latest']=df["""('Latest', "('Equities', 'NetNotional')")"""]+0.35*df["""('Latest', "('Credit', 'NetNotional')")"""]
#     df['net_exp_adj_latest'].replace(0, np.nan, inplace=True)
#     return df

def _ar_xs_ret_summary(df):
    ar_xs_ret_sum=df[["('AbsoluteReturnBenchmarkExcess', 'YTD')",
                          "('AbsoluteReturnBenchmarkExcess', 'TTM')",
                          "('AbsoluteReturnBenchmarkExcess', '3Y')",
                          "('AbsoluteReturnBenchmarkExcess', '5Y')",
                          "('AbsoluteReturnBenchmarkExcess', '10Y')",
                          "('AbsoluteReturnBenchmarkExcess', 'ITD')"]]
    #ar_xs_ret_sum_df = ar_xs_ret_sum_df.mul(100)
    return ar_xs_ret_sum

def _xs_emm_rank_ptile_summary(df):
    df['3Y_ptiles'] = pd.DataFrame(df['3y_ptiles'].apply(lambda x: x*100).round(0).astype(pd.Int64Dtype()))
    df['5Y_ptiles'] = pd.DataFrame(df['5Y_ptiles_y'].apply(lambda x: x*100).round(0))
    df['10Y_ptiles'] = pd.DataFrame(df['10Y_ptiles'].apply(lambda x: x*100).round(0))
    df['TTM_ptiles'] = pd.DataFrame(df['TTM_ptiles'].apply(lambda x: x*100).round(0))
    xs_emm_ptile_sum=df[["TTM_ptiles",
                          "3Y_ptiles",
                          "5Y_ptiles",
                          "10Y_ptiles"]]
    return xs_emm_ptile_sum

def _non_factor_rba_summary(df):
    rba_risk_decomp_sum=df[["('NON_FACTOR_RISK', '3Y')"]]
    return rba_risk_decomp_sum

def _gcm_peer_ptile_summary(df):
    peer_ptile_sum=df[["TTM_peer_ptiles",
                          "3Y_peer_ptiles",
                          "5Y_peer_ptiles",
                          "10Y_peer_ptiles"]]
    return peer_ptile_sum

def _gcm_peer_screener_rank(df):
    gcm_peer_screener_sum=df[["Decile_x",
                          "Confidence_x",
                          "Persistence_x"]]
    return gcm_peer_screener_sum

def _net_equivs_exposure_summary(df):
    net_equivs_sxp_sum=df[['net_exp_adj_latest',
                          'net_exp_adj_3y',
                          'net_exp_adj_5y']]
    return net_equivs_sxp_sum

def _shortfall_pass_fail_summary(df, wl, alloc_status, close_end):
    shortfall_sum=df[['InvestmentGroupName','Pass/Fail', 'Drawdown']]
    shortfall_sum=shortfall_sum.merge(wl, on='InvestmentGroupName', how='left')
    shortfall_sum['status']=shortfall_sum['IsWatchList'].replace([True], 'WL')
    shortfall_sum=shortfall_sum.replace(False, np.nan)
    shortfall_sum=shortfall_sum.merge(alloc_status, on='InvestmentGroupName', how='left')
    shortfall_sum['status']=shortfall_sum['status'].fillna(shortfall_sum['Acronym'])
    shortfall_sum=shortfall_sum.drop(['Drawdown','IsWatchList','Acronym'], axis=1)
    
    shortfall_sum=shortfall_sum.merge(close_end, on='InvestmentGroupName', how='left')
    shortfall_sum['status']=shortfall_sum['status'].fillna(shortfall_sum['ce_status'])
    shortfall_sum=shortfall_sum.drop(['ce_status', 'InvestmentGroupName'], axis=1)
    return shortfall_sum

def _lagging_quarter_ptiles(df):
    df['ITD_lag_tmp']=pd.DataFrame(df["('AbsoluteReturnBenchmarkExcessLag', 'ITD')"])
    df['ITD_ptiles_default_lag'] = pd.DataFrame(df.ITD_lag_tmp).rank(numeric_only=True, pct = True)
    
    df['default_3y_lag'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcessLag', '3Y')"])
    
    df['3y_lag_ptiles'] = pd.DataFrame(df.default_3y_lag).rank(numeric_only=True, pct = True)

    df['3y_lag_ptiles']=df['3y_lag_ptiles'].fillna(df['ITD_ptiles_default_lag'])

    return df

def _lagging_quarter_ptiles_summary(df):
    df['3y_lag_ptiles'] = pd.DataFrame(df['3y_lag_ptiles'].apply(lambda x: x*100).round(0))
    
    quarter_lagging_ptile_sum=df[["3y_lag_ptiles"]]

    return quarter_lagging_ptile_sum

def _arb_definition(df):
    arb_definition=df[['absolute_return_benchmark']]
    return arb_definition

def _status_wl():
    include_filters = dict(status=["EMM"])
    exclude_filters = dict(strategy=["Other", "Aggregated Prior Period Adjustment"])
    exclude_gcm_portfolios = True

    fund_dimn = InvestmentGroup(investment_group_ids=None).get_dimensions(
        exclude_gcm_portfolios=exclude_gcm_portfolios,
        include_filters=include_filters,
        exclude_filters=exclude_filters,
    )


    wl=fund_dimn[['InvestmentGroupName', 'IsWatchList']]
    return wl

def _status_close_end():
    close_end=InvestmentGroup(investment_group_ids=None).get_attributes_long()
    close_end=close_end[((close_end['Field']=='IsDrawdownFund') & (close_end['Value']=='1')) | 
        ((close_end['Field']=='IsInvestable') & (close_end['Value']=='0')) | 
        ((close_end['Field']=='IsSLF') & (close_end['Value']=='1'))]
    close_end=close_end.drop_duplicates(subset=['InvestmentGroupName'])
    close_end['ce_status']='CE'
    close_end=close_end.drop(['Field', 'Value'], axis=1)
    return close_end

def _status_alloc(df, as_of_date):
    start_date=as_of_date.replace(day=1)

    with Scenario(as_of_date=dt.date.today()).context():
        portfolio = Portfolio()

    holdings = portfolio.get_holdings(start_date=start_date,
                                          end_date=as_of_date)

    portfolio_allocation_status=holdings[['Acronym', 'InvestmentGroupName']]
    
    #fill status column with portfolio_allocation_status if empty
    df=df.merge(portfolio_allocation_status, on='InvestmentGroupName', how='left')

    portfolio_allocation_status=df[['InvestmentGroupName', 'Acronym']]
    portfolio_allocation_status=portfolio_allocation_status.groupby("InvestmentGroupName", group_keys=True).apply(lambda x: x)
    portfolio_allocation_status=portfolio_allocation_status.drop_duplicates(subset=['InvestmentGroupName'], keep=False)
    portfolio_allocation_status=portfolio_allocation_status[(portfolio_allocation_status['Acronym']=='GMSF') | (portfolio_allocation_status['Acronym']=='SPECTRUM')]

    status_alloc=portfolio_allocation_status[['InvestmentGroupName','Acronym']]
    status_alloc.index.names = ['ind1','ind2']
    return status_alloc
    
def _status_options(df, as_of_date):
    wl=_status_wl()
    status_alloc=_status_alloc(df=df, as_of_date=as_of_date)
    close_end=_status_close_end()
    return wl, status_alloc, close_end

def _summarize_data(df, as_of_date):

    status_hierarchy = _status_options(df=df, as_of_date=as_of_date)
    inv_group_basics=df[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation']]
    shortfall_sum=_shortfall_pass_fail_summary(df=df, wl=status_hierarchy[0], alloc_status=status_hierarchy[1], close_end=status_hierarchy[2])
    arb_definition=_arb_definition(df=df)
    xs_return_sum=_ar_xs_ret_summary(df=df)
    xs_emm_rank_sum=_xs_emm_rank_ptile_summary(df=df)
    rba_sum=_non_factor_rba_summary(df=df)
    peer_rank_ptiles=_gcm_peer_ptile_summary(df=df)
    peer_screener_rank=_gcm_peer_screener_rank(df=df)
    exposure_sum=_net_equivs_exposure_summary(df=df)
    lagging_q_ptile_sum=_lagging_quarter_ptiles_summary(df=df)
    
    summary_list_df=[inv_group_basics, shortfall_sum, arb_definition, xs_return_sum, xs_emm_rank_sum,
                     rba_sum, peer_rank_ptiles, peer_screener_rank, exposure_sum,
                     lagging_q_ptile_sum]
    summary=pd.concat(summary_list_df, axis=1)

    return summary

def _get_high_performing_summary(df):
    high_perf=df
    high_perf=high_perf.loc[(df['3Y_ptiles'] >= 75) | (df['5Y_ptiles'] >= 77)]
    high_perf=high_perf.iloc[::-1]
    high_perf['Pass/Fail']=""
    high_perf_sum=high_perf[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark']]
    high_perf_data=high_perf.drop(['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark'], axis=1)
    return high_perf_sum,high_perf_data


def _get_low_performing_summary(df):
    low_perf=df.loc[(df['3Y_ptiles'] <= 25) | (df['5Y_ptiles'] <= 25) | (df['Pass/Fail'] == 'Fail')]
    low_perf_sum=low_perf[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark']]
    low_perf_data=low_perf.drop(['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark'], axis=1)
    return low_perf_sum, low_perf_data


def _xpfund_data_to_highlow_df(df: pd.DataFrame,
                               as_of_date,
                               periods: List[str]=['TTM', '5Y', '10Y']):
    df_fw_cleaned = _clean_firmwide_xpfund_data(df=df)

    df_xs = _3y_arb_xs_analysis(df=df_fw_cleaned)
    df_xs.rename(columns={
        "('Peer1Ptile', 'TTM')": 'TTM_peer_ptiles',
        "('Peer1Ptile', '3Y')": '3Y_peer_ptiles',
        "('Peer1Ptile', '5Y')": '5Y_peer_ptiles',
        "('Peer1Ptile', '10Y')": '10Y_peer_ptiles'
    }, inplace=True)
    
    df_pctiles = reduce(
        lambda left, right: pd.merge(
            left, right, on='InvestmentGroupName', how="outer"
        ),
        [
            _arb_xs_emm_percentiles(df_xs, period=i) for i in periods

        ],
    )
    df_pctiles_q_lag = _lagging_quarter_ptiles(df=df_pctiles)
    # df_pctiles_with_exp = _net_exp_adj_3y(df=df_pctiles_q_lag)
    # df_pctiles_with_exp = _net_exp_adj_5y(df=df_pctiles_with_exp)
    # df_pctiles_with_exp_latest = _net_exp_adj_latest(df=df_pctiles_with_exp)
    
    df_pctiles_with_exposures = _net_exposure_clean(df=df_pctiles_q_lag)
    
    df = _summarize_data(df=df_pctiles_with_exposures, as_of_date=as_of_date)
    high_perf=_get_high_performing_summary(df=df)
    low_perf=_get_low_performing_summary(df=df)


    return high_perf[0], high_perf[1], low_perf[0], low_perf[1]
