import pandas as pd
from typing import List
from pyxirr import xirr
import numpy as np
from dateutil.relativedelta import relativedelta
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
import datetime as dt
from functools import reduce

def __runner() -> DaoRunner:
    return Scenario.get_attribute("dao")

def discount_table(
    dates_cashflows,
    cashflows,
    cashflows_type,
    dates_index,
    index,
    NAV_scaling=1,
):
    """Automatically matches cashflow and index dates and subsequently generates discount table (which can be used to calculate Direct Alpha et al.).
        Also useful for debugging and exporting.

    Args:
        dates_cashflows: An ndarray the dates corresponding to cashflows.
        cashflows: An ndarray of the cashflow amounts (sign does not matter)
        cashflows_type: Accepts three types [Distribution \ Capital Call \ Value]
        dates_index: Ndarray of dates for the index, same logic as cashflows.
        index: The index levels corresponding to the dates
        NAV_scaling: Coefficient which can be used to scale the NAV amount (so as to counteract systemic mispricing)
        auto_NAV: Toggle for automatic handling of the NAV. If False, NAV is not calculated and function returns a tuple of [sum_fv_distributions, sum_fv_calls]
                    (allows for manual completion of the PME formula using appropriate NAV value)
    Returns:
        DataFrame(Date|Amount|Type|Status|Discounted|Index|FV_Factor)
    """
    _dates_index = pd.to_datetime(dates_index)
    df_cf = pd.concat(
        [pd.to_datetime(dates_cashflows), cashflows, cashflows_type],
        axis=1,
    )
    df_cf.columns = ["Date", "Amount", "Type"]
    df_cf = df_cf.sort_values("Date")
    df_cf = df_cf.reset_index(drop=True)
    # Get NAV
    if df_cf[
        (df_cf["Type"] == "Value") & (df_cf["Amount"] == 0)
    ].empty:  # Checks if liquidated by looking at 0 valuations
        NAV_record = (
            df_cf[df_cf["Type"] == "Value"]
            .sort_values("Date", ascending=False)
            .head(1)
            .copy()
        )
        NAV_date = NAV_record["Date"].iloc[0]
        NAV_index_value = index[
            _dates_index == nearest(_dates_index, NAV_date)
        ].iloc[0]
        df_cf["Status"] = "Active"
    else:  # Not liquidated
        NAV_record = (
            df_cf[df_cf["Type"] != "Value"]
            .sort_values("Date", ascending=False)
            .head(1)
            .copy()
        )
        NAV_record["Amount"].iloc[0] = 0  # force a 0 value
        NAV_date = NAV_record["Date"].iloc[0]
        NAV_index_value = index[
            _dates_index == nearest(_dates_index, NAV_date)
        ].iloc[0]
        df_cf["Status"] = "Liquidated"
    # Iterate and assign to table
    df_cf["Pre-Discounted"] = 0
    df_cf["Discounted"] = 0
    df_cf["Index"] = 0
    df_cf["Index_date"] = 0
    df_cf["FV_Factor"] = 0
    for idx, cf in df_cf.iterrows():
        # Let us find the closest index value to the current date
        index_date = nearest(_dates_index, cf["Date"])
        index_value = index[_dates_index == index_date].iloc[0]
        df_cf.loc[idx, "Index"] = index_value
        df_cf.loc[idx, "Index_date"] = index_date
        df_cf.loc[idx, "FV_Factor"] = NAV_index_value / index_value
        if cf["Type"] == "Distribution":
            df_cf.loc[idx, "Discounted"] = cf["Amount"] * (
                NAV_index_value / index_value
            )
            df_cf.loc[idx, "Pre-Discounted"] = cf["Amount"]
        elif cf["Type"] == "Capital Call":
            df_cf.loc[idx, "Discounted"] = cf["Amount"] * (
                NAV_index_value / index_value
            )
            df_cf.loc[idx, "Pre-Discounted"] = cf["Amount"]

    # Attach relevant NAV value
    df_cf.loc[
        (df_cf["Date"] == NAV_date) & (df_cf["Type"] == "Value"),
        "Discounted",
    ] = (
        NAV_record["Amount"].iloc[0] * NAV_scaling
    )
    df_cf.loc[
        (df_cf["Date"] == NAV_date) & (df_cf["Type"] == "Value"),
        "Pre-Discounted",
    ] = (
        NAV_record["Amount"].iloc[0] * NAV_scaling
    )

    # cut table at FV date
    df_cf = df_cf[df_cf["Date"] <= NAV_date].copy()
    return df_cf.copy()

def get_investment_sector_benchmark(df):
    df_bmark_mapped = df.copy()
    # only SPXT currently per Amy
    df_bmark_mapped["BenchmarkTicker"] = "SPXT Index"

    def oper(query: Query, item: DeclarativeMeta):
        query = filter_many(query, item, f"Ticker", list(df_bmark_mapped.BenchmarkTicker.unique()))
        return query

    p = {
        "table": "FactorReturns",
        "schema": "analyticsdata",
        "operation": lambda query, items: oper(query, items),
    }
    index_prices = __runner().execute(
        params=p,
        source=DaoSource.InvestmentsDwh,
        operation=lambda d, pp: d.get_data(pp),
    )
    index_prices_rslt = index_prices[['Ticker', 'Date', 'PxLast']].pivot(
        index="Date", columns="Ticker", values="PxLast"
    ).reset_index()

    return df_bmark_mapped, index_prices_rslt


def format_and_get_pme_bmarks(df: pd.DataFrame,
                                _attributes_needed: List[str]):
    # prep data
    fund_cf = df[
        _attributes_needed
        + ["TransactionDate", "TransactionType", "BaseAmount"]
    ]
    #TODO: really need to standardize transaction type tags or use enums
    fund_cf.TransactionType = np.select([
        (fund_cf.TransactionType == "Distributions"),
        (fund_cf.TransactionType == "Contributions"),
        (fund_cf.TransactionType == "Net Asset Value"),
    ], ["Distribution", "Capital Call", "Value"])

    # get index prices
    fund_cf_bmark, index_prices = get_investment_sector_benchmark(fund_cf)
    return fund_cf_bmark, index_prices

def get_alpha_discount_table(fund_df, fund_cf, index_prices):
    discount_table_rslt = pd.DataFrame()
    for idx in range(0, len(fund_df)):
        print(fund_df.Name[idx])
        single_fund = fund_cf[
            fund_cf["Name"] == fund_df.Name[idx]
        ].copy()
        single_fund_group_sum = (
            single_fund.groupby(
                ["Name", "TransactionDate", "TransactionType"]
            )
            .sum()
            .reset_index()
        )
        if len(single_fund_group_sum.TransactionDate.unique()) < 2:
            continue
        max_nav_date = single_fund_group_sum[
            single_fund_group_sum.TransactionType == "Value"
        ].TransactionDate.max()
        total_nav = single_fund_group_sum[
            single_fund_group_sum.TransactionType == "Value"
        ].BaseAmount.sum()
        single_fund_group_sum = pd.concat(
            [
                single_fund_group_sum[
                    single_fund_group_sum.TransactionType != "Value"
                ],
                pd.DataFrame(
                    {
                        "Name": single_fund_group_sum.Name.unique(),
                        "TransactionDate": [max_nav_date],
                        "TransactionType": "Value",
                        "BaseAmount": total_nav,
                    }
                ),
            ]
        )
        fund_specific_index = index_prices[
            ["Date", fund_df.BenchmarkTicker[idx]]
        ]

        assert len(fund_specific_index.columns) == 2
        assert len(single_fund_group_sum.Name.unique()) == 1
        assert (
            len(
                single_fund_group_sum[
                    single_fund_group_sum.TransactionType == "Value"
                ]
            )
            == 1
        )
        assert (
            single_fund_group_sum[
                single_fund_group_sum.TransactionType == "Value"
            ].TransactionDate.max()
            == single_fund.TransactionDate.max()
        )

        discount_table_df = discount_table(
            dates_cashflows=single_fund_group_sum["TransactionDate"],
            cashflows=single_fund_group_sum["BaseAmount"],
            cashflows_type=single_fund_group_sum["TransactionType"],
            dates_index=fund_specific_index.iloc[:, 0],
            index=fund_specific_index.iloc[:, 1],
        )
        discount_table_df["Name"] = fund_df.Name[idx]
        discount_table_rslt = pd.concat(
            [discount_table_rslt, discount_table_df]
        )
    return discount_table_rslt

def nearest(series, lookup, debug=False):
    if debug == True:
        print(
            "lookup: "
            + str(lookup)
            + "  | closest: "
            + str(series.iloc[(series - lookup).abs().argsort()[0]])
        )
    return series.iloc[(series - lookup).abs().argsort()[0]]

def get_direct_alpha_rpt(
    as_of_date: dt.date,
    df: pd.DataFrame,
    nav_df: pd.DataFrame,
    list_to_iterate: List[List[str]],
    _attributes_needed: List[str],
    _trailing_periods: dict,
):
    # bmark assignment is always at investment level
    fund_cf, index_prices = format_and_get_pme_bmarks(df, _attributes_needed)
    fund_df = (
        fund_cf[_attributes_needed + ["BenchmarkTicker"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    discount_df = get_alpha_discount_table(
        fund_df=fund_df, fund_cf=fund_cf, index_prices=index_prices
    )

    result = pd.DataFrame()
    for group_cols in list_to_iterate:
        for trailing_period in list(_trailing_periods.keys()):
            if trailing_period in ["YTD", "QTD", "TTM"]:
                continue
            print(f"{group_cols} -  {trailing_period}")
            group_cols_date = group_cols.copy()
            group_cols_date.extend(["Date"])

            if trailing_period != "ITD":
                start_date = as_of_date + relativedelta(
                    months=(
                        _trailing_periods.get(trailing_period) * -3
                    ),
                    days=1,
                )
                starting_investment = nav_df[
                    nav_df.TransactionDate
                    == pd.to_datetime(start_date + relativedelta(days=-1))
                ]
                if len(starting_investment) == 0:
                    continue

                # starting_investment = starting_investment.groupby(group_cols).BaseAmount.sum().reset_index()
                starting_investment.BaseAmount = (
                    starting_investment.BaseAmount * -1
                )
                starting_investment["Date"] = pd.to_datetime(start_date)

                index_start_value = index_prices[
                    index_prices.Date
                    == nearest(index_prices.Date, start_date)
                ].iloc[0]

                index_end_value = index_prices[
                    index_prices.Date
                    == nearest(
                        index_prices.Date, as_of_date
                    )
                ].iloc[0]

                initial_fv_factor = (
                    index_end_value[1] / index_start_value[1]
                )
                starting_investment["Discounted"] = (
                    starting_investment.BaseAmount * initial_fv_factor
                )

                grouped_filtered = discount_df[
                    discount_df.Date >= pd.to_datetime(start_date)
                ]
                grouped_filtered = pd.concat(
                    [starting_investment, grouped_filtered]
                )

            else:
                grouped_filtered = discount_df.copy()
                starting_investment = None

            discount_df_with_attrib = grouped_filtered[
                ["Name", "Date", "Discounted"]
            ].merge(
                df[
                    _attributes_needed + ["Portfolio"]
                ].drop_duplicates(),
                how="left",
                left_on="Name",
                right_on="Name",
            )
            assert len(grouped_filtered) == len(discount_df_with_attrib)

            if starting_investment is not None:
                # TODO can be done much cleaner...
                # filter out items that don't meet full trailing period
                full_period_groups = list(
                    starting_investment.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    )
                )
                discount_df_with_attrib = discount_df_with_attrib[
                    discount_df_with_attrib.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    ).isin(full_period_groups)
                ]

            rslt = discount_df_with_attrib.groupby(group_cols)[
                ["Date", "Discounted"]
            ].apply(xirr, silent=True)
            rslt = rslt.reset_index().rename(columns={0: "Irr"})
            rslt["DirectAlpha"] = np.log(1 + rslt.Irr)

            rslt["Name"] = rslt.apply(
                lambda x: "_".join([str(x[i]) for i in group_cols]),
                axis=1,
            )
            rslt["Period"] = trailing_period
            rslt = rslt[
                ["Name", "DirectAlpha", "Period"]
            ].drop_duplicates()
            result = pd.concat([result, rslt])[
                ["Name", "DirectAlpha", "Period"]
            ]

    return result, discount_df

def get_ks_pme_rpt(
        as_of_date: dt.date,
        df: pd.DataFrame,
        nav_df: pd.DataFrame,
        list_to_iterate: List[List[str]],
        _attributes_needed: List[str],
        _trailing_periods: dict) -> pd.DataFrame:
    # bmark assignment is always at investment level
    fund_cf, index_prices = format_and_get_pme_bmarks(df, _attributes_needed)
    fund_df = (
        fund_cf[_attributes_needed + ["BenchmarkTicker"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    result = pd.DataFrame()
    for trailing_period in list(_trailing_periods.keys()):
        print(trailing_period)
        if trailing_period in ["YTD", "QTD", "TTM"]:
            continue
        if trailing_period != "ITD":
            start_date = as_of_date + relativedelta(
                months=(
                    _trailing_periods.get(trailing_period) * -3
                ),
                days=1,
            )
            starting_investment = nav_df[
                nav_df.TransactionDate
                == pd.to_datetime(start_date + relativedelta(days=-1))
            ]
            # starting_investment = starting_investment.groupby(group_cols).BaseAmount.sum().reset_index()
            starting_investment.BaseAmount = (
                starting_investment.BaseAmount * -1
            )
            starting_investment["Date"] = pd.to_datetime(start_date)
            starting_investment["TransactionType"] = "Capital Call"

            fund_cf_filtered = fund_cf[
                fund_cf.apply(lambda x: pd.Timestamp(x)) >= pd.to_datetime(start_date)
            ]
            fund_cf_filtered = pd.concat(
                [starting_investment, fund_cf_filtered]
            )
        else:
            fund_cf_filtered = fund_cf
            starting_investment = None

        fv_cashflows_df = get_fv_cashflow_df(
            fund_df=fund_df,
            fund_cf=fund_cf_filtered,
            index_prices=index_prices,
        )
        if len(fv_cashflows_df) == 0:
            continue
        fv_cashflows_df_with_attrib = fv_cashflows_df.merge(
            df[
                _attributes_needed + ["Portfolio"]
            ].drop_duplicates(),
            how="left",
            left_on="Name",
            right_on="Name",
        )
        assert len(fv_cashflows_df) == len(fv_cashflows_df_with_attrib)

        for group_cols in list_to_iterate:
            if starting_investment is not None:
                # TODO can be done much cleaner...
                # filter out items that don't meet full trailing period
                full_period_groups = list(
                    starting_investment.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    )
                )

                fv_cashflows_df_with_attrib = (
                    fv_cashflows_df_with_attrib[
                        fv_cashflows_df_with_attrib.apply(
                            lambda x: "_".join(
                                [str(x[i]) for i in group_cols]
                            ),
                            axis=1,
                        ).isin(full_period_groups)
                    ]
                )

            rslt = (
                fv_cashflows_df_with_attrib.groupby(group_cols)
                .sum()
                .reset_index()
            )
            rslt["KsPme"] = (
                rslt.sum_fv_distributions + rslt.discounted_nav
            ) / rslt.sum_fv_calls
            rslt["Name"] = rslt.apply(
                lambda x: "_".join([str(x[i]) for i in group_cols]),
                axis=1,
            )
            rslt["Period"] = trailing_period

            result = pd.concat([result, rslt])[
                ["Name", "KsPme", "Period"]
            ]

    return result

def get_fv_cashflow_df(fund_df: pd.DataFrame,
                       fund_cf: pd.DataFrame,
                       index_prices: pd.DataFrame):
    fv_cashflows_df = pd.DataFrame()
    for idx in range(0, len(fund_df)):
        print(fund_df.Name[idx])

        single_fund = fund_cf[
            fund_cf["Name"] == fund_df.Name[idx]
        ].copy()
        # if len(single_fund) <= 1 or (single_fund.BaseAmount.sum() <= 1) & (single_fund.BaseAmount.sum() >= -1):
        if len(single_fund) <= 1:
            continue
        if (
            len(single_fund[single_fund.TransactionType == "Value"])
            == 0
        ):
            continue
        if len(single_fund.TransactionDate.unique()) < 2:
            continue
        single_fund_group_sum = (
            single_fund.groupby(
                ["Name", "TransactionDate", "TransactionType"]
            )
            .sum()
            .reset_index()
        )
        max_nav_date = single_fund_group_sum[
            single_fund_group_sum.TransactionType == "Value"
        ].TransactionDate.max()
        total_nav = single_fund_group_sum[
            single_fund_group_sum.TransactionType == "Value"
        ].BaseAmount.sum()
        single_fund_group_sum = pd.concat(
            [
                single_fund_group_sum[
                    single_fund_group_sum.TransactionType != "Value"
                ],
                pd.DataFrame(
                    {
                        "Name": single_fund_group_sum.Name.unique(),
                        "TransactionDate": [max_nav_date],
                        "TransactionType": "Value",
                        "BaseAmount": total_nav,
                    }
                ),
            ]
        )
        fund_specific_index = index_prices[
            ["Date", fund_df.BenchmarkTicker[idx]]
        ]

        assert len(fund_specific_index.columns) == 2
        assert len(single_fund_group_sum.Name.unique()) == 1
        assert (
            len(
                single_fund_group_sum[
                    single_fund_group_sum.TransactionType == "Value"
                ]
            )
            == 1
        )
        assert (
            single_fund_group_sum[
                single_fund_group_sum.TransactionType == "Value"
            ].TransactionDate.max()
            == single_fund.TransactionDate.max()
        )

        fv_cashflows = KS_PME(
            dates_cashflows=single_fund_group_sum["TransactionDate"],
            cashflows=single_fund_group_sum["BaseAmount"],
            cashflows_type=single_fund_group_sum["TransactionType"],
            dates_index=fund_specific_index.iloc[:, 0],
            index=fund_specific_index.iloc[:, 1],
            auto_NAV=False,
        )
        index_value = fund_specific_index[
            fund_specific_index.Date
            == nearest(
                fund_specific_index.Date, max_nav_date
            )
        ][fund_df.BenchmarkTicker[idx]].iloc[0]
        discounted_NAV = total_nav / index_value.squeeze()
        fv_cashflows_df = pd.concat(
            [
                fv_cashflows_df,
                pd.DataFrame(
                    {
                        "Name": single_fund_group_sum.Name.unique(),
                        "sum_fv_distributions": fv_cashflows[0],
                        "sum_fv_calls": fv_cashflows[1],
                        "discounted_nav": discounted_NAV,
                    }
                ),
            ]
        )
    return fv_cashflows_df

def KS_PME(
    dates_cashflows,
    cashflows,
    cashflows_type,
    dates_index,
    index,
    NAV_scaling=1,
    auto_NAV=True,
):
    """Calculates the Kalpan Schoar PME. Designed for plug & play with Preqin data.
    Args:
        dates_cashflows: An ndarray the dates corresponding to cashflows.
        cashflows: An ndarray of the cashflow amounts (sign does not matter)
        cashflows_type: Accepts three types [Distribution \ Capital Call \ Value]
        dates_index: Ndarray of dates for the index, same logic as cashflows.
        index: The index levels corresponding to the dates
        NAV_scaling: Coefficient which can be used to scale the NAV amount (so as to counteract systemic mispricing)
        auto_NAV: Toggle for automatic handling of the NAV. If False, NAV is not calculated and function returns a tuple of [sum_fv_distributions, sum_fv_calls]
                    (allows for manual completion of the PME formula using appropriate NAV value)
    Returns:
        The KS-PME metric given the inputed index
    """
    _dates_index = pd.to_datetime(dates_index)
    df_cf = pd.concat(
        [pd.to_datetime(dates_cashflows), cashflows, cashflows_type],
        axis=1,
    )
    df_cf.columns = ["Date", "Amount", "Type"]
    # first let us run through the cashflow data and sum up all of the calls and distributions
    sum_fv_distributions = 0
    sum_fv_calls = 0
    for idx, cf in df_cf.iterrows():
        # Let us find the closest index value to the current date
        index_value = index[
            _dates_index == nearest(_dates_index, cf["Date"])
        ].iloc[0]
        if cf["Type"] == "Distribution":
            sum_fv_distributions = (
                sum_fv_distributions + abs(cf["Amount"]) / index_value
            )
        elif cf["Type"] == "Capital Call":
            sum_fv_calls = (
                sum_fv_calls + abs(cf["Amount"]) / index_value
            )
            # Now, let us also consider the nav
    if auto_NAV == True:
        # Let us find the nav
        NAV_record = (
            df_cf[df_cf["Type"] == "Value"]
            .sort_values("Date", ascending=False)
            .head(1)
        )
        index_value = index[
            _dates_index
            == nearest(_dates_index, NAV_record["Date"].iloc[0])
        ].iloc[0]
        discounted_NAV = (
            NAV_record["Amount"].iloc[0] / index_value
        ) * NAV_scaling
        # return according to the KSPME formula
        return (sum_fv_distributions + discounted_NAV) / sum_fv_calls
    else:
        return [sum_fv_distributions, sum_fv_calls]

def get_ror_ctr_df_rpt(as_of_date: dt.date,
                       df: pd.DataFrame,
                       owner: str,
                       list_to_iterate: List[List[str]],
                       _attributes_needed: List[str],
                       _trailing_periods: dict) -> pd.DataFrame:
    ror_ctr_df = pd.concat(
        [
            get_ror_ctr(
                as_of_date=as_of_date,
                df=df,
                group_cols=i,
                trailing_periods=_trailing_periods,
                _attributes_needed=_attributes_needed
            )
            for i in list_to_iterate
        ]
    )[["Name", "AnnRor", "Ctr", "Period", "group_cols"]]
    result = pd.DataFrame()
    for i in ror_ctr_df.Period.unique():
        sub = ror_ctr_df[ror_ctr_df.Period == i]
        for x in sub.group_cols.unique():
            subb = sub[sub.group_cols == x]
            ctr_total = sub[
                sub.Name == owner
            ].AnnRor.squeeze() / sum(subb[~subb.Ctr.isnull()].Ctr)
            subb["Ctr"] = subb.Ctr * ctr_total.squeeze()
            result = pd.concat([result, subb])[
                ["Name", "AnnRor", "Ctr", "Period"]
            ]
    return result

def get_ror_ctr(as_of_date: dt.date,
                df: pd.DataFrame,
                group_cols: List[str],
                _attributes_needed: List[str],
                trailing_periods: dict):
    rors = calc_tw_ror(
        df=df,
        group_cols=group_cols,
        _attributes_needed=_attributes_needed,
        return_support_data=True
    )
    if len(rors) == 0:
        return pd.DataFrame(
            columns=["Name", "Period", "AnnRor", "Ctr", "group_cols"]
        )

    ann_ror = ann_return(
        as_of_date=as_of_date,
        return_df=rors.pivot_table(index="Date", columns="Name", values="Ror"),
        trailing_periods=trailing_periods,
        freq=4
    )

    ctr = get_ctrs(
        as_of_date=as_of_date,
        ctrs=rors.pivot_table(
            index="Date", columns="Name", values="Ctr"
        ).fillna(0),
        trailing_periods=trailing_periods,
    )

    result = ann_ror.merge(
        ctr,
        how="outer",
        left_on=["Name", "Period"],
        right_on=["Name", "Period"],
    )
    result["group_cols"] = str(group_cols)
    return result

def get_ctrs(as_of_date: dt.date,
             ctrs: pd.DataFrame,
             trailing_periods: dict):
    def safe_division(numerator, denominator):
        """Return 0 if denominator is 0."""
        return denominator and numerator / denominator

    result = pd.DataFrame()
    for trailing_period in trailing_periods.keys():
        if trailing_period != "ITD":
            start_date = as_of_date + relativedelta(
                months=-3
                * (trailing_periods.get(trailing_period)),
                days=1,
            )
            ctr = calc_ctr(
                ctrs.loc[start_date:as_of_date]
            )
            ctr.AnnCtr = ctr[["Ctr", "NoObs"]].apply(
                lambda row: (1 + row["Ctr"])
                ** (safe_division(4, row["NoObs"]))
                - 1,
                axis=1,
            )
            ctr.Ctr = np.where(ctr.NoObs <= 4, ctr.Ctr, ctr.AnnCtr)
        else:
            ctr = calc_ctr(ctrs)
            ctr.AnnCtr = ctr[["Ctr", "NoObs"]].apply(
                lambda row: (1 + row["Ctr"])
                ** (safe_division(4, row["NoObs"]))
                - 1,
                axis=1,
            )
            ctr.Ctr = np.where(ctr.NoObs <= 4, ctr.Ctr, ctr.AnnCtr)
        ctr["Period"] = trailing_period
        result = pd.concat([result, ctr])
    return result

def calc_ctr(df: pd.DataFrame):
    result = pd.DataFrame(columns=["Ctr", "NoObs"], index=df.columns)
    rors = df.sum(axis=1)

    for col in df.columns:
        ctr = df.loc[:, col].tolist()
        ror_to_date = (1 + rors).cumprod() - 1
        idx = 1
        res = ctr[0]
        for c in ctr[1:]:
            res += c * (1 + ror_to_date[idx - 1])
            idx += 1
        result.loc[col, "Ctr"] = res
        result.loc[col, "NoObs"] = len(
            df.loc[:, col][df.loc[:, col] != 0]
        )

    return result

def ann_return(
        as_of_date: dt.date,
        return_df: pd.DataFrame,
        trailing_periods: dict,
        freq=4,
        return_NoObs=True
):
    trailing_periods_obs = [
       trailing_periods[x] for x in trailing_periods
    ]
    result = pd.DataFrame()
    for i in return_df.columns:
        returns = return_df[[i]]
        returns = returns.dropna()
        if (
            len(trailing_periods_obs) == 1
            and trailing_periods_obs[0] != "ITD"
            and len(returns) < trailing_periods_obs[0]
        ):
            ann_return_df = pd.DataFrame(
                {
                    returns.columns.name: [None],
                    "AnnRor": [None],
                    "NoObs": [None],
                }
            )
            if return_NoObs:
                result = pd.concat([result, ann_return_df])
            else:
                ann_return_df.drop(columns=["NoObs"])
                result = pd.concat([result, ann_return_df])

        for period in trailing_periods.keys():
            trailing_period = trailing_periods.get(period)
            if trailing_period != "ITD":
                if len(returns) < trailing_period:
                    continue
                else:
                    start_date = as_of_date + relativedelta(
                        months=-3 * (trailing_period), days=1
                    )
                    return_sub = returns.loc[
                        start_date : as_of_date
                    ]
            else:
                return_sub = returns
            if len(return_sub) <= 4:
                ann_return = pd.DataFrame(
                    pd.DataFrame((1 + return_sub).prod() - 1)
                )
            else:
                ann_return = pd.DataFrame(
                    return_sub.add(1).prod()
                    ** (freq / len(return_sub))
                    - 1
                )
            ann_return["NoObs"] = len(return_sub)
            ann_return["Period"] = period
            result = pd.concat([result, ann_return])
    result = result.reset_index().rename(
        columns={"index": "Name", 0: "AnnRor"}
    )
    if return_NoObs:
        return result
    else:
        return result.drop(columns=["NoObs"])

def get_last_day_of_prior_quarter(p_date):
    return dt.date(
        p_date.year, 3 * ((p_date.month - 1) // 3) + 1, 1
    ) + dt.timedelta(days=-1)

def get_last_day_of_the_quarter(p_date):
    quarter = (p_date.month - 1) // 3 + 1
    return dt.date(
        p_date.year + 3 * quarter // 12, 3 * quarter % 12 + 1, 1
    ) + dt.timedelta(days=-1)

def calc_tw_ror(df: pd.DataFrame,
                group_cols: List[str],
                _attributes_needed: List[str],
                return_support_data=False):
    df = df[df.TransactionDate <= df.MaxNavDate]
    df = df.sort_values("TransactionDate")
    df["lastq"] = df.TransactionDate.apply(
        lambda row: get_last_day_of_prior_quarter(row)
    )
    df["thisq"] = df.TransactionDate.apply(
        lambda row: get_last_day_of_the_quarter(row)
    )
    df["DateDiff"] = (df.thisq - df.TransactionDate) + dt.timedelta(
        days=1
    )
    df["TotalDays"] = df.thisq - df.lastq

    df["Weight"] = df.DateDiff / df.TotalDays
    df["Weight"] = np.where(
        df.TransactionType == "Net Asset Value", 0, df.Weight
    )
    df["wAmount"] = df.Weight * df.BaseAmount

    nav = df[df.TransactionType == "Net Asset Value"].rename(
        columns={"BaseAmount": "Nav"}
    )
    nav = nav.merge(
        nav[
            _attributes_needed
            + [
                "OwnerName",
                "InvestmentName",
                "PredominantStrategy",
                "thisq",
                "Nav",
            ]
        ].rename(columns={"Nav": "PriorNav", "thisq": "lastq"}),
        how="left",
        left_on=_attributes_needed
        + ["OwnerName", "InvestmentName", "PredominantStrategy", "lastq"],
        right_on=_attributes_needed
        + ["OwnerName", "InvestmentName", "PredominantStrategy", "lastq"],
    )
    cf = df[df.TransactionType != "Net Asset Value"]

    data = pd.concat([nav, cf])

    lowest_group = group_cols.copy()
    if "Name" not in group_cols:
        lowest_group.extend(["Name"])

    data_sum_lowest = (
        data[
            lowest_group
            + ["thisq", "Nav", "PriorNav", "BaseAmount", "wAmount"]
        ]
        .groupby(lowest_group + ["thisq"])
        .sum()
        .reset_index()
    )

    data_sum_lowest["EndingNavAdj"] = (
        data_sum_lowest.Nav + data_sum_lowest.BaseAmount
    )
    data_sum_lowest["PriorNavAdj"] = (
        data_sum_lowest.PriorNav - data_sum_lowest.wAmount
    )
    data_sum_lowest[
        [
            "Nav",
            "PriorNav",
            "BaseAmount",
            "wAmount",
            "EndingNavAdj",
            "PriorNavAdj",
        ]
    ] = data_sum_lowest[
        [
            "Nav",
            "PriorNav",
            "BaseAmount",
            "wAmount",
            "EndingNavAdj",
            "PriorNavAdj",
        ]
    ].fillna(
        0
    )
    data_sum_lowest = data_sum_lowest[data_sum_lowest.PriorNavAdj > 0]
    data_sum_lowest = data_sum_lowest[data_sum_lowest.Nav > 0]
    data_sum_lowest = data_sum_lowest[data_sum_lowest.PriorNav > 0]
    data_sum = (
        data_sum_lowest[
            group_cols
            + [
                "thisq",
                "Nav",
                "PriorNav",
                "BaseAmount",
                "wAmount",
                "EndingNavAdj",
                "PriorNavAdj",
            ]
        ]
        .groupby(group_cols + ["thisq"])
        .sum()
        .reset_index()
    )

    data_sum["Ror"] = (
        data_sum.EndingNavAdj - data_sum.PriorNav
    ) / data_sum.PriorNavAdj
    data_sum["Gain"] = data_sum.EndingNavAdj - data_sum.PriorNav

    navAdj = (
        data_sum[["thisq", "PriorNavAdj"]]
        .groupby(["thisq"])
        .sum()
        .reset_index()
        .rename(columns={"PriorNavAdj": "TotalPriorNav"})
    )

    rslt = data_sum.merge(
        navAdj, how="left", left_on="thisq", right_on="thisq"
    )
    rslt["Alloc"] = rslt.PriorNavAdj / rslt.TotalPriorNav
    rslt["Ctr"] = rslt.Alloc * rslt.Ror

    rslt["Name"] = rslt.apply(
        lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
    )
    rslt.rename(columns={"thisq": "Date"}, inplace=True)

    if return_support_data:
        return rslt
    else:
        return rslt[["Date", "Name", "Ror", "Alloc", "Ctr"]]

def get_horizon_irr_df_rpt(
        as_of_date: dt.date,
        df: pd.DataFrame,
        nav_df: pd.DataFrame,
        list_to_iterate: List[List[str]],
        _attributes_needed: List[str],
        _trailing_periods: dict) -> pd.DataFrame:
    result = pd.DataFrame()
    for trailing_period in list(_trailing_periods.keys()):
        print(trailing_period)
        if trailing_period in ["YTD", "QTD", "TTM"]:
            continue
        if trailing_period != "ITD":
            start_date = as_of_date + relativedelta(
                months=(
                    _trailing_periods.get(trailing_period) * -3
                ),
                days=1,
            )
            starting_investment = nav_df[
                nav_df.TransactionDate
                == pd.to_datetime(start_date + relativedelta(days=-1))
            ]
            # starting_investment = starting_investment.groupby(group_cols).BaseAmount.sum().reset_index()
            starting_investment.BaseAmount = (
                starting_investment.BaseAmount * -1
            )
            starting_investment["Date"] = pd.to_datetime(start_date)
            starting_investment["TransactionType"] = "Contributions"

            fund_cf_filtered = df[
                df.TransactionDate >= pd.to_datetime(start_date)
            ]
            fund_cf_filtered = pd.concat(
                [starting_investment, fund_cf_filtered]
            )
        else:
            fund_cf_filtered = df
            starting_investment = None

        for group_cols in list_to_iterate:
            if starting_investment is not None:
                # TODO can be done much cleaner...
                # filter out items that don't meet full trailing period
                full_period_groups = list(
                    starting_investment.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    )
                )

                fund_cf_filtered = fund_cf_filtered[
                    fund_cf_filtered.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    ).isin(full_period_groups)
                ]

            irr_data = calc_irr(
                cf=fund_cf_filtered,
                group_cols=group_cols
            )[["Name", "GrossIrr"]]
            irr_data["NoObs"] = trailing_period
            result = pd.concat([result, irr_data])

    return result

def calc_irr(cf: pd.DataFrame,
             group_cols=List[str],
             type="Gross"):
    # all funds/deals in cfs dataframe are what the result will reflect (i.e. do filtering beforehand)
    if len(cf) == 0:
        return pd.DataFrame(columns=["Name", type + "Irr"])
    if group_cols is None:
        irr = xirr(
            cf[["TransactionDate", "BaseAmount"]]
            .groupby("TransactionDate")
            .sum()
            .reset_index(drop=True).squeeze()
        )
    else:
        irr = cf.groupby(group_cols)[
            ["TransactionDate", "BaseAmount"]
        ].apply(xirr, silent=True)
        irr = irr.reset_index().rename(columns={0: type + "Irr"})
        irr["Name"] = irr.apply(
            lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
        )
    return irr

def get_horizon_tvpi_df_rpt(
        as_of_date: dt.date,
        df: pd.DataFrame,
        nav_df: pd.DataFrame,
        list_to_iterate: List[List[str]],
        _attributes_needed: List[str],
        _trailing_periods: dict) -> pd.DataFrame:
    result = pd.DataFrame()
    for trailing_period in list(_trailing_periods.keys()):
        print(trailing_period)
        if trailing_period in ["YTD", "QTD", "TTM"]:
            continue
        if trailing_period != "ITD":
            start_date = as_of_date + relativedelta(
                months=(
                    _trailing_periods.get(trailing_period) * -3
                ),
                days=1,
            )
            starting_investment = nav_df[
                nav_df.TransactionDate
                == pd.to_datetime(start_date + relativedelta(days=-1))
            ]
            # starting_investment = starting_investment.groupby(group_cols).BaseAmount.sum().reset_index()
            starting_investment.BaseAmount = (
                starting_investment.BaseAmount * -1
            )
            starting_investment["Date"] = pd.to_datetime(start_date)
            starting_investment["TransactionType"] = "Contributions"

            fund_cf_filtered = df[
                df.TransactionDate >= pd.to_datetime(start_date)
            ]
            fund_cf_filtered = pd.concat(
                [starting_investment, fund_cf_filtered]
            )
        else:
            fund_cf_filtered = df.copy()
            starting_investment = None

        for group_cols in list_to_iterate:
            if starting_investment is not None:
                # TODO can be done much cleaner...
                # filter out items that don't meet full trailing period
                full_period_groups = list(
                    starting_investment.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    )
                )

                fund_cf_filtered = fund_cf_filtered[
                    fund_cf_filtered.apply(
                        lambda x: "_".join(
                            [str(x[i]) for i in group_cols]
                        ),
                        axis=1,
                    ).isin(full_period_groups)
                ]

            multiple_df = calc_multiple(
                fund_cf_filtered, group_cols=group_cols
            )[["Name", "GrossMultiple"]]
            multiple_df["NoObs"] = trailing_period
            result = pd.concat([result, multiple_df])

    return result

def calc_multiple(
        cf: pd.DataFrame,
        group_cols=List[str],
        type="Gross"):

    # all funds/deals in cfs dataframe are what the result will reflect (i.e. do filtering beforehand)
    if len(cf) == 0:
        return pd.DataFrame(columns=["Name", type + "Multiple"])
    if group_cols is None:
        multiple = cf[
            cf.TransactionType.isin(
                ["Distributions", "Net Asset Value"]
            )
        ].BaseAmount.sum() / abs(
            cf[
                cf.TransactionType.isin(["Contributions"])
            ].BaseAmount.sum()
        )
    else:
        multiple = (
            cf[
                cf.TransactionType.isin(
                    ["Distributions", "Net Asset Value"]
                )
            ]
            .groupby(group_cols)
            .BaseAmount.sum()
            / cf[cf.TransactionType.isin(["Contributions"])]
            .groupby(group_cols)
            .BaseAmount.sum()
            .abs()
        )
        multiple = multiple.reset_index().rename(
            columns={"BaseAmount": type + "Multiple"}
        )
        multiple["Name"] = multiple.apply(
            lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
        )
    return multiple

def get_sum_df_rpt(df, list_to_iterate):
    sum_df = pd.concat(
        [calc_sum(df, group_cols=i) for i in list_to_iterate]
    )
    return sum_df

def calc_sum(df, group_cols):
    rslt = df.groupby(group_cols).sum().reset_index()
    rslt["Name"] = rslt.apply(
        lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
    )
    return rslt

def get_holding_periods_rpt(df, discount_df, list_to_iterate, _attributes_needed):
    max_nav_date = (
        df[df.TransactionType == "Net Asset Value"]
        .groupby(["Name"])
        .TransactionDate.max()
        .reset_index()
        .rename(columns={"TransactionDate": "MaxNavDate"})
    )

    min_cf_date = (
        df[_attributes_needed + ["TransactionDate", "Portfolio"]]
        .groupby(_attributes_needed + ["Portfolio"])
        .min()
        .reset_index()
    )
    date_df = min_cf_date.merge(max_nav_date, how="outer")
    date_df["HoldingPeriod"] = (
        date_df.MaxNavDate - date_df.TransactionDate
    ) / pd.Timedelta("365 days")

    rslt = pd.concat(
        [
            calc_duration(discount_df, group_cols=i)
            for i in list_to_iterate
        ]
    )
    rslt = rslt[["Name", "Duration"]]

    # used for big join... remove
    rslt["NoObs"] = "Incep"
    return rslt, max_nav_date

def pivot_trailing_period_df(df):
    df_melted = pd.melt(df, id_vars=["Name", "Period"])
    df_melted = df_melted.pivot_table(
        index="Name", columns=["Period", "variable"], values=["value"]
    ).reset_index()
    return df_melted

def calc_duration(discount_df, group_cols):
    def convert_back_to_date(date):
        date = pd.to_numeric(date, errors="coerce")
        if np.isnan(date) or date is None:
            return None
        else:
            return dt.date.fromordinal(int(date))

    def convert_timedelta_to_years(date_diff):
        return None if date_diff is None else date_diff.days / 365

    group_cols_extended = group_cols.copy()
    group_cols_extended.extend(["Date", "Type"])
    df = (
        discount_df.groupby(group_cols_extended)
        .Discounted.sum()
        .reset_index()
    )
    df["DateInt"] = [i.toordinal() for i in df.Date]
    df["DurationCtr"] = df.DateInt * df.Discounted

    inflows = df[df.Type == "Capital Call"]
    inflows = (
        inflows.groupby(group_cols).DurationCtr.sum()
        / inflows.groupby(group_cols).Discounted.sum()
    ).reset_index()
    inflows["avg_inflow_date"] = inflows[0].apply(convert_back_to_date)

    outflows = df[df.Type.isin(["Distribution", "Value"])]
    outflows = (
        outflows.groupby(group_cols).DurationCtr.sum()
        / outflows.groupby(group_cols).Discounted.sum()
    ).reset_index()
    outflows["avg_outflow_date"] = outflows[0].apply(
        convert_back_to_date
    )

    inflows.drop(0, inplace=True, axis=1)
    outflows.drop(0, inplace=True, axis=1)
    df_dates = inflows.merge(outflows, how="outer").drop_duplicates()
    df_dates["DateDiff"] = (
        df_dates.avg_outflow_date - df_dates.avg_inflow_date
    )
    df_dates["Duration"] = df_dates.DateDiff.apply(
        convert_timedelta_to_years
    )

    df_dates["Name"] = df_dates.apply(
        lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
    )
    return df_dates

def recurse_down_order(
    df: pd.DataFrame,
    group_by_list: List[str],
    depth: int,
    counter: int,
) -> pd.DataFrame:
    final_df_cache = []
    if df.shape[0] > 0:
        current_grouping_struct = df.groupby(
            group_by_list[depth], sort=False
        )
        for name, group in current_grouping_struct:
            if depth == 4:
                print("smart guy")
            simple_frame = pd.DataFrame(
                {
                    "DisplayName": [name],
                    "Layer": [depth],
                    "Name": [
                        group.reset_index()["Group" + str(depth)][0]
                    ],
                    "Description": [
                        group.reset_index()[
                            "Group"
                            + str(depth - 1 if depth != 0 else 0)
                        ][0]
                    ],
                    "Counter": [counter],
                }
            )
            final_df_cache.append(simple_frame)

            assert type(group) is pd.DataFrame
            reset = depth + 1
            if group.shape[0] > 0 and depth < len(group_by_list) - 1:
                children_data_frame, counter = recurse_down_order(
                    df=group,
                    group_by_list=group_by_list,
                    depth=reset,
                    counter=counter - 1,
                )
                if children_data_frame is not None:
                    final_df_cache.append(children_data_frame)
            else:
                if (
                    len(final_df_cache) == len(current_grouping_struct)
                ) & (depth < len(group_by_list)):
                    counter = counter - 1
                else:
                    pd.concat(final_df_cache), counter

    if len(final_df_cache) > 0:
        return pd.concat(final_df_cache), counter
    return None, counter

