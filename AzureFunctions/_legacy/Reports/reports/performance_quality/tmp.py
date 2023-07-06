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
    df = df.dropna(subset=["('AbsoluteReturnBenchmarkExcess', 'MTD')",
                         "('AbsoluteReturnBenchmarkExcess', 'ITD')"], 
                 how='all'
                   )
    return df


def _3y_arb_xs_analysis(df):
    #copy_df=df.copy()
    #copy_df["filled_3y"]=df["('AbsoluteReturnBenchmarkExcess', '3Y')"] if not nan, 
    #   else df["('AbsoluteReturnBenchmarkExcess', 'ITD')"]

    df['default_3y'] = pd.DataFrame(df["('AbsoluteReturnBenchmarkExcess', '3Y')"])
    df['default_3y'] = df['default_3y'].fillna(df["('AbsoluteReturnBenchmarkExcess', 'ITD')"])

    #sort by default_3y
    df = df.sort_values(by='default_3y').reset_index()
    df = df.drop('index', axis=1)

    return df


def _3y_arb_xs_emm_percentiles(df):
    #3y_filled_list=df['filled_3y'].tolist()
    #df["3y_ptiles"]=percentileofscore(3y_filled_list, df["filled_3y"], kind='rank')
    #x.loc[:, 'pcta'] = x.rank(pct=True)
    #df['3y_ptiles'] = df.filled_3y.rank(pct = True)
    df['copy_default_3y'] = pd.DataFrame(df['default_3y'])
    df['3y_ptiles'] = pd.DataFrame(df.copy_default_3y).rank(numeric_only=True, pct = True)
    df = df.drop('copy_default_3y', axis=1)
    df = df.drop('default_3y', axis=1)
    df_high = df.tail(40)
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
    return df

def _net_exp_adj_5y(df):
    return df

def _net_exp_adj_latest(df):
    return df

def _summarize_data(df):
    return df

def _get_highlow_performing_df(df):
    #high: if 3y_ptile >= 75 or 5y_ptile >= 75
    #low: if 3y_ptile <= 25 or 5y_ptile <=25
    #high_df=pd.DataFrame()
    #low_df=pd.DataFrame()
    #iterate thru rows of df, add to high or low depending on condition
    #ret[0]=high_df, ret[1]=low_df
    return 1,2


def _get_low_performing_df(df):
    return df


def _xpfund_data_to_highlow_df(df):
    df = _clean_firmwide_xpfund_data(df=df)
    df = _3y_arb_xs_analysis(df=df)
    df = _fund_return_peer_percentiles(df)
    df = _3y_arb_xs_emm_percentiles(df=df)
    df = _5y_arb_xs_emm_percentiles(df=df)
    df = _10y_arb_xs_emm_percentiles(df=df)
    df = _TTM_arb_xs_emm_percentiles(df=df)
    df = _net_exp_adj_3y(df=df)
    df = _net_exp_adj_5y(df=df)
    df = _net_exp_adj_latest(df=df)
    df = _summarize_data(df=df)
    df_high_performance = _get_highlow_performing_df(df=df)[0]
    df_low_performance = _get_highlow_performing_df(df=df)[1]
     

    #a=_3y_arb_xs_analysis(df)
    #b=_3y_arb_xs_emm_percentiles(a)
    #c=_5y_arb_xs_emm_percentiles(b)
    #d,e=_get_high_performing_df(c)

    #('NON_FACTOR_RISK', 'TTM')

    return df
