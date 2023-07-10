import datetime as dt
import numpy as np
import pandas as pd
import scipy
from gcm.inv.scenario import Scenario
from scipy.stats import spearmanr, percentileofscore


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
    return df

def _net_exp_adj_5y(df):
    df["""('5Y', "('Equities', 'NetNotional')")"""]=df["""('5Y', "('Equities', 'NetNotional')")"""].fillna(0)
    df["""('5Y', "('Credit', 'NetNotional')")"""]=df["""('5Y', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_5y']=df["""('5Y', "('Equities', 'NetNotional')")"""]+0.35*df["""('5Y', "('Credit', 'NetNotional')")"""]
    return df

def _net_exp_adj_latest(df):
    df["""('Latest', "('Equities', 'NetNotional')")"""]=df["""('Latest', "('Equities', 'NetNotional')")"""].fillna(0)
    df["""('Latest', "('Credit', 'NetNotional')")"""]=df["""('Latest', "('Credit', 'NetNotional')")"""].fillna(0)
    df['net_exp_adj_latest']=df["""('Latest', "('Equities', 'NetNotional')")"""]+0.35*df["""('Latest', "('Credit', 'NetNotional')")"""]
    return df

def _ar_xs_ret_summary(df):
    ar_xs_ret_sum=df[["('AbsoluteReturnBenchmarkExcess', 'YTD')",
                          "('AbsoluteReturnBenchmarkExcess', 'TTM')",
                          "('AbsoluteReturnBenchmarkExcess', '3Y')",
                          "('AbsoluteReturnBenchmarkExcess', '5Y')",
                          "('AbsoluteReturnBenchmarkExcess', '10Y')",
                          "('AbsoluteReturnBenchmarkExcess', 'ITD')"]]
    return ar_xs_ret_sum

def _xs_emm_rank_ptile_summary(df):
    xs_emm_ptile_sum=df[["TTM_ptiles",
                          "3y_ptiles",
                          "5y_ptiles",
                          "10y_ptiles"]]
    return xs_emm_ptile_sum

def _non_factor_rba_summary(df):
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

def _shortfall_pass_fail_summary(df):
    shortfall_sum=df[['Pass/Fail', 'Drawdown']]
    #shortfall_sum['Pass/Fail']=np.nan
    shortfall_sum['Drawdown']=""
    return shortfall_sum

def _arb_definition(df):
    arb_definition=df[['absolute_return_benchmark']]
    #shortfall_sum['Pass/Fail']=np.nan
    #shortfall_sum['Drawdown']=""
    return arb_definition


def _summarize_data(df):
    sum0=df[['InvestmentGroupName','ReportingPeerGroup','FirmwideAllocation']]
    sum7=_shortfall_pass_fail_summary(df=df)
    sum8=_arb_definition(df=df)
    sum1=_ar_xs_ret_summary(df=df)
    sum2=_xs_emm_rank_ptile_summary(df=df)
    sum3=_non_factor_rba_summary(df=df)
    sum4=_gcm_peer_ptile_summary(df=df)
    sum5=_gcm_peer_screener_rank(df=df)
    sum6=_net_equivs_exposure_summary(df=df)
    
    summary=pd.concat([sum0, sum7], axis=1)
    summary=pd.concat([summary, sum8], axis=1)
    summary=pd.concat([summary, sum1], axis=1)
    summary=pd.concat([summary, sum2], axis=1)
    summary=pd.concat([summary, sum3], axis=1)
    summary=pd.concat([summary, sum4], axis=1)
    summary=pd.concat([summary, sum5], axis=1)
    summary=pd.concat([summary, sum6], axis=1)
    #summary=pd.concat([summary, sum7], axis=1)
    return summary

def _get_high_performing_summary(df):
    #high_perf=df.drop(df.columns[len(df.columns)-1], axis=1)
    high_perf=df
    high_perf=high_perf.loc[(df['3y_ptiles'] >= 0.75) | (df['5y_ptiles'] >= 0.77)]
    high_perf=high_perf.iloc[::-1]
    high_perf['Pass/Fail']=""
    #high_perf = high_perf.drop('rank_3y_ptiles', axis=1)
    #high_perf['Pass/Fail'] = high_perf['Pass/Fail'].replace('Pass', np.nan)
    #high_perf['Drawdown'] = high_perf['Drawdown'].replace('value', np.nan)
    #df[apply(df>10,1,any),]
    return high_perf


def _get_low_performing_summary(df):
    #low_perf=df
    low_perf=df.loc[(df['3y_ptiles'] <= 0.25) | (df['5y_ptiles'] <= 0.25) | (df['Pass/Fail'] == 'Fail')]
    #low_perf = low_perf.drop('rank_3y_ptiles', axis=1)
    #ow_perf['Drawdown'] = low_perf['Drawdown'].replace('value', np.nan)
    #low_perf=low_perf.iloc[::-1]
    #df[apply(df>10,1,any),]
    return low_perf


def _xpfund_data_to_highlow_df(df):
    df = _clean_firmwide_xpfund_data(df=df)
    df = _3y_arb_xs_analysis(df=df)
    df = _fund_return_peer_percentiles(df=df)
    df = _3y_arb_xs_emm_percentiles(df=df)
    df = _5y_arb_xs_emm_percentiles(df=df)
    df = _10y_arb_xs_emm_percentiles(df=df)
    df = _TTM_arb_xs_emm_percentiles(df=df)
    df = _net_exp_adj_3y(df=df)
    df = _net_exp_adj_5y(df=df)
    df = _net_exp_adj_latest(df=df)
    
    df = _summarize_data(df=df)
    high_perf=_get_high_performing_summary(df=df)
    low_perf=_get_low_performing_summary(df=df)


    return high_perf, low_perf
