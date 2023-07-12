import datetime as dt
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
    # df = df.dropna(subset=["('AbsoluteReturnBenchmarkExcess', 'MTD')",
    #                      "('AbsoluteReturnBenchmarkExcess', 'ITD')"], 
    #              how='all'
    #                )
    return df


def _3y_arb_xs_analysis(df):
    #copy_df=df.copy()
    #copy_df["filled_3y"]=df["('AbsoluteReturnBenchmarkExcess', '3Y')"] if not nan, 
    #   else df["('AbsoluteReturnBenchmarkExcess', 'ITD')"]

    df['ITD_tmp']=pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', 'ITD')"])
    df['ITD_ptiles_default'] = pd.DataFrame(df.ITD_tmp).rank(numeric_only=True, pct = True)
    
    df['default_3y'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', '3Y')"])
    
    df['3y_ptiles'] = pd.DataFrame(df.default_3y).rank(numeric_only=True, pct = True)
    #df['rank_3y_ptiles']=df['3y_ptiles'].copy()
    df['3y_ptiles']=df['3y_ptiles'].fillna(df['ITD_ptiles_default'])
    #df['3y_ptiles'] = df['3y_ptiles'].fillna(df['ITD_ptiles_default'])
    
    #df['default_3y'] = df['default_3y'].fillna(df["('AbsoluteReturnBenchmarkExcess', 'ITD')"])

    #sort by default_3y
    #df = df.sort_values(by='default_3y').reset_index()
    df = df.sort_values(by='3y_ptiles').reset_index()
    #df = df.dropna(subset=['3y_ptiles'])
    #df = df.drop('index', axis=1)

    return df


def _3y_arb_xs_emm_percentiles(df):
    # df['copy_default_3y'] = pd.DataFrame(df['default_3y'])
    # df['3y_ptiles'] = pd.DataFrame(df.copy_default_3y).rank(numeric_only=True, pct = True)
    # df = df.drop('copy_default_3y', axis=1)
    # df = df.drop('default_3y', axis=1)
    # df_high = df.tail(40)
    return df


def _5y_arb_xs_emm_percentiles(df):
    df['copy_5y'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', '5Y')"])
    df['5y_ptiles'] = pd.DataFrame(df.copy_5y).rank(numeric_only=True, pct = True)
    df = df.drop('copy_5y', axis=1)
    return df


def _10y_arb_xs_emm_percentiles(df):
    df['copy_10y'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', '10Y')"])
    df['10y_ptiles'] = pd.DataFrame(df.copy_10y).rank(numeric_only=True, pct = True)
    df = df.drop('copy_10y', axis=1)
    return df


def _TTM_arb_xs_emm_percentiles(df):
    df['copy_TTM'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', 'TTM')"])
    df['TTM_ptiles'] = pd.DataFrame(df.copy_TTM).rank(numeric_only=True, pct = True)
    df = df.drop('copy_TTM', axis=1)
    return df


def _fund_return_peer_percentiles(df):
    
    df['TTM_peer_ptiles']=pd.DataFrame(df["('Peer1Ptile', 'TTM')"])
    df['3y_peer_ptiles']=pd.DataFrame(df["('Peer1Ptile', '3Y')"])
    df['5y_peer_ptiles']=pd.DataFrame(df["('Peer1Ptile', '5Y')"])
    df['10y_peer_ptiles']=pd.DataFrame(df["('Peer1Ptile', '10Y')"])
    x=df.tail(20)

    return df

def _net_exp_adj_3y(df):
    #df['net_exp_adj_3y']=df[('3Y', "('Equities', 'NetNotional')")]+0.35*df[('3Y', "('Credit', 'NetNotional')")]
    df["""('3Y', "('Equities', 'NetNotional')")"""]=df["""('3Y', "('Equities', 'NetNotional')")"""].fillna(0)
    df["""('3Y', "('Credit', 'NetNotional')")"""]=df["""('3Y', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_3y']=df["""('3Y', "('Equities', 'NetNotional')")"""]+0.35*df["""('3Y', "('Credit', 'NetNotional')")"""]
    #df['net_exp_adj_3y'] = pd.DataFrame(df['net_exp_adj_3y'].apply(lambda x: x*100).round(0).astype(int))
    df['net_exp_adj_3y'].replace(0, np.nan, inplace=True)
    return df

def _net_exp_adj_5y(df):
    df["""('5Y', "('Equities', 'NetNotional')")"""]=df["""('5Y', "('Equities', 'NetNotional')")"""].fillna(0)
    df["""('5Y', "('Credit', 'NetNotional')")"""]=df["""('5Y', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_5y']=df["""('5Y', "('Equities', 'NetNotional')")"""]+0.35*df["""('5Y', "('Credit', 'NetNotional')")"""]
    #df['net_exp_adj_5y'] = pd.DataFrame(df['net_exp_adj_5y'].apply(lambda x: x*100).round(0).astype(int))
    df['net_exp_adj_5y'].replace(0, np.nan, inplace=True)
    return df

def _net_exp_adj_latest(df):
    df["""('Latest', "('Equities', 'NetNotional')")"""]=df["""('Latest', "('Equities', 'NetNotional')")"""].fillna(0)
    df["""('Latest', "('Credit', 'NetNotional')")"""]=df["""('Latest', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_latest']=df["""('Latest', "('Equities', 'NetNotional')")"""]+0.35*df["""('Latest', "('Credit', 'NetNotional')")"""]
    #df['net_exp_adj_latest'] = pd.DataFrame(df['net_exp_adj_latest'].apply(lambda x: x*100).round(0).astype(int))
    df['net_exp_adj_latest'].replace(0, np.nan, inplace=True)
    return df

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
    df['3y_ptiles'] = pd.DataFrame(df['3y_ptiles'].apply(lambda x: x*100).round(0).astype(pd.Int64Dtype()))
    df['5y_ptiles'] = pd.DataFrame(df['5y_ptiles'].apply(lambda x: x*100).round(0))
    df['10y_ptiles'] = pd.DataFrame(df['10y_ptiles'].apply(lambda x: x*100).round(0))
    df['TTM_ptiles'] = pd.DataFrame(df['TTM_ptiles'].apply(lambda x: x*100).round(0))
    #df['5y_ptiles'] = pd.DataFrame(df['5y_ptiles'].apply(lambda x: x*100  if(np.all(pd.notna(x))) else x).round(0).astype(int))
    #df['5y_ptiles'] = pd.to_numeric(df['5y_ptiles'])
    # df['10y_ptiles'] = pd.DataFrame(df['10y_ptiles'].apply(lambda x: x*100).round(0).astype(pd.Int64Dtype()))
    # df['TTM_ptiles'] = pd.DataFrame(df['TTM_ptiles'].apply(lambda x: x*100).round(0).astype(pd.Int64Dtype()))
    #df=df.round({'3y_ptiles': 0,'5y_ptiles': 0,'10y_ptiles': 0,'TTM_ptiles': 0,}).astype(int)
    xs_emm_ptile_sum=df[["TTM_ptiles",
                          "3y_ptiles",
                          "5y_ptiles",
                          "10y_ptiles"]]
    return xs_emm_ptile_sum

def _non_factor_rba_summary(df):
    #df["('NON_FACTOR_RISK', '3Y')"] = pd.DataFrame(df["('NON_FACTOR_RISK', '3Y')"].apply(lambda x: x*100).round(0).astype(pd.Int64Dtype()))
    #df["('NON_FACTOR_RISK', '3Y')"] = pd.DataFrame(df["('NON_FACTOR_RISK', '3Y')"].apply(lambda x: x*100).round(0).astype(int))
    rba_risk_decomp_sum=df[["('NON_FACTOR_RISK', '3Y')"]]
    return rba_risk_decomp_sum

def _gcm_peer_ptile_summary(df):
    peer_ptile_sum=df[["TTM_peer_ptiles",
                          "3y_peer_ptiles",
                          "5y_peer_ptiles",
                          "10y_peer_ptiles"]]
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
    #shortfall_sum['Drawdown']=""
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
    #shortfall_sum['Pass/Fail']=np.nan
    #shortfall_sum['Drawdown']=""
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
    #fund_dimn.drop(fund_dimn[fund_dimn['IsWatchList']==False].index)['IsWatchList']
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

def test_get_holdings(df, as_of_date):
    start_date=as_of_date.replace(day=1)
    #acronyms = ["GMSF", "SPECTRUM"]

    with Scenario(as_of_date=dt.date.today()).context():
        portfolio = Portfolio()
        #portfolio = Portfolio(acronyms=acronyms)
    # holdings = portfolio.get_holdings(start_date=dt.date(2022, 1, 1),
    #                                       end_date=dt.date(2022, 3, 1))

    holdings = portfolio.get_holdings(start_date=start_date,
                                          end_date=as_of_date)

    portfolio_allocation_status=holdings[['Acronym', 'InvestmentGroupName']]
    
    #fill status column with portfolio_allocation_status if empty
    df=df.merge(portfolio_allocation_status, on='InvestmentGroupName', how='left')

    portfolio_allocation_status=df[['InvestmentGroupName', 'Acronym']]
    portfolio_allocation_status=portfolio_allocation_status.groupby("InvestmentGroupName", group_keys=True).apply(lambda x: x)
    portfolio_allocation_status=portfolio_allocation_status.drop_duplicates(subset=['InvestmentGroupName'], keep=False)
    portfolio_allocation_status=portfolio_allocation_status[(portfolio_allocation_status['Acronym']=='GMSF') | (portfolio_allocation_status['Acronym']=='SPECTRUM')]
    #only_alloc_status={'GMSF', 'SPECTRUM'}
    #portfolio_allocation_status=portfolio_allocation_status.groupby('InvestmentGroupName').filter(lambda g: only_alloc_status.issubset(g['Acronym']))
    status_alloc=portfolio_allocation_status[['InvestmentGroupName','Acronym']]
    status_alloc.index.names = ['ind1','ind2']
    return status_alloc
    
def _summarize_data(df, as_of_date):
    x=_status_wl()
    y=test_get_holdings(df=df, as_of_date=as_of_date)
    z=_status_close_end()
    sum0=df[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation']]
    sum7=_shortfall_pass_fail_summary(df=df, wl=x, alloc_status=y, close_end=z)
    sum8=_arb_definition(df=df)
    sum1=_ar_xs_ret_summary(df=df)
    sum2=_xs_emm_rank_ptile_summary(df=df)
    sum3=_non_factor_rba_summary(df=df)
    sum4=_gcm_peer_ptile_summary(df=df)
    sum5=_gcm_peer_screener_rank(df=df)
    sum6=_net_equivs_exposure_summary(df=df)
    sum9=_lagging_quarter_ptiles_summary(df=df)
    
    summary=pd.concat([sum0, sum7], axis=1)
    summary=pd.concat([summary, sum8], axis=1)
    summary=pd.concat([summary, sum1], axis=1)
    summary=pd.concat([summary, sum2], axis=1)
    summary=pd.concat([summary, sum3], axis=1)
    summary=pd.concat([summary, sum4], axis=1)
    summary=pd.concat([summary, sum5], axis=1)
    summary=pd.concat([summary, sum6], axis=1)
    summary=pd.concat([summary, sum9], axis=1)
    #summary=pd.concat([summary, sum7], axis=1)
    return summary

def _get_high_performing_summary(df):
    #high_perf=df.drop(df.columns[len(df.columns)-1], axis=1)
    
    high_perf=df
    high_perf=high_perf.loc[(df['3y_ptiles'] >= 75) | (df['5y_ptiles'] >= 77)]
    high_perf=high_perf.iloc[::-1]
    high_perf['Pass/Fail']=""
    high_perf_sum=high_perf[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark']]
    high_perf_data=high_perf.drop(['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark'], axis=1)
    #df = df.drop('copy_5y', axis=1)
    #high_perf = high_perf.drop('rank_3y_ptiles', axis=1)
    #high_perf['Pass/Fail'] = high_perf['Pass/Fail'].replace('Pass', np.nan)
    #high_perf['Drawdown'] = high_perf['Drawdown'].replace('value', np.nan)
    #df[apply(df>10,1,any),]
    return high_perf_sum,high_perf_data


def _get_low_performing_summary(df):
    #low_perf=df
    low_perf=df.loc[(df['3y_ptiles'] <= 25) | (df['5y_ptiles'] <= 25) | (df['Pass/Fail'] == 'Fail')]
    #low_perf = low_perf.drop('rank_3y_ptiles', axis=1)
    #ow_perf['Drawdown'] = low_perf['Drawdown'].replace('value', np.nan)
    #low_perf=low_perf.iloc[::-1]
    #df[apply(df>10,1,any),]
    low_perf_sum=low_perf[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark']]
    low_perf_data=low_perf.drop(['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation','Pass/Fail', 'status', 'absolute_return_benchmark'], axis=1)
    return low_perf_sum, low_perf_data


def _xpfund_data_to_highlow_df(df, as_of_date):
    #x=test_get_holdings(df=df, as_of_date=as_of_date)
    #x=_status_close_end()
    df = _clean_firmwide_xpfund_data(df=df)
    df = _3y_arb_xs_analysis(df=df)
    df = _fund_return_peer_percentiles(df=df)
    df = _3y_arb_xs_emm_percentiles(df=df)
    df = _5y_arb_xs_emm_percentiles(df=df)
    df = _10y_arb_xs_emm_percentiles(df=df)
    df = _TTM_arb_xs_emm_percentiles(df=df)
    df=_lagging_quarter_ptiles(df)
    df = _net_exp_adj_3y(df=df)
    df = _net_exp_adj_5y(df=df)
    df = _net_exp_adj_latest(df=df)
    
    df = _summarize_data(df=df, as_of_date=as_of_date)
    high_perf=_get_high_performing_summary(df=df)
    low_perf=_get_low_performing_summary(df=df)


    return high_perf[0], high_perf[1], low_perf[0], low_perf[1]
