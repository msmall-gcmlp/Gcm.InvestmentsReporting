import datetime as dt
import numpy as np
import pandas as pd
import scipy
from gcm.inv.scenario import Scenario
from scipy.stats import spearmanr, percentileofscore

def _3y_arb_xs_analysis(df):
    #copy_df=df.copy()
    #copy_df["filled_3y"]=df["('AbsoluteReturnBenchmarkExcess', '3Y')"] if not nan, 
             #   else df["('AbsoluteReturnBenchmarkExcess', 'ITD')"]
    
    return

def _3y_arb_xs_emm_percentiles(df):
    #3y_filled_list=df['filled_3y'].tolist()
    #df["3y_ptiles"]=percentileofscore(3y_filled_list, df["filled_3y"], kind='rank')
    #x.loc[:, 'pcta'] = x.rank(pct=True)
    #df['3y_ptiles'] = df.filled_3y.rank(pct = True)
    return

def _5y_arb_xs_emm_percentiles(df):
    df['x']=df["('AbsoluteReturnBenchmarkExcess', '5Y')"]
    df['5y_ptiles'] = df.x.rank(pct = True)
    return df

def _get_high_performing_df(df):
    #high: if 3y_ptile >= 75 or 5y_ptile >= 75
    #low: if 3y_ptile <= 25 or 5y_ptile <=25
    #high_df=pd.DataFrame()
    #low_df=pd.DataFrame()
    #iterate thru rows of df, add to high or low depending on condition
    #ret[0]=high_df, ret[1]=low_df
    return

def _get_low_performing_df(df):
    return

def _xpfund_data_to_highlow_df(df):
    #a=_3y_arb_xs_analysis(df)
    #b=_3y_arb_xs_emm_percentiles(a)
    #c=_5y_arb_xs_emm_percentiles(b)
    #d,e=_get_high_performing_df(c)
    return 