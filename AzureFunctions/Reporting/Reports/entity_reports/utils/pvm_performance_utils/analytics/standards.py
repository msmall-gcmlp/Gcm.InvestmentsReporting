import pandas as pd
from typing import List
import pyxirr as xirr
import numpy as np
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from datetime import relativedelta


def __runner() -> DaoRunner:
    return Scenario.get_attribute("dao")


def get_investment_sector_benchmark(df):
    df_bmark_mapped = df
    df_bmark_mapped["BenchmarkTicker"] = "SPXT Index"

    def my_dao_operation(dao, params):
        raw = (
            f"select Ticker, Date, PxLast from analyticsdata.FactorReturns"
            f" where Ticker in {list(df_bmark_mapped.BenchmarkTicker.unique())}"
        )
        raw = raw.replace("[", "(")
        raw = raw.replace("]", ")")
        df = pd.read_sql(
            raw,
            dao.data_engine.session.bind,
        )
        return df

    index_prices = __runner().execute(
        params={},
        source=DaoSource.InvestmentsDwh,
        operation=my_dao_operation,
    )
    index_prices_rslt = index_prices.pivot(
        index="Date", columns="Ticker", values="PxLast"
    ).reset_index()

    return df_bmark_mapped, index_prices_rslt


def format_and_get_bmark_ks_pme(df, _attributes_needed: List[str]):
    # prep data
    fund_cf = df[
        _attributes_needed
        + ["TransactionDate", "TransactionType", "BaseAmount"]
    ]

    conditions = [
        (fund_cf.TransactionType == "Distributions"),
        (fund_cf.TransactionType == "Contributions"),
        (fund_cf.TransactionType == "Net Asset Value"),
    ]
    new_type = ["Distribution", "Capital Call", "Value"]
    fund_cf.TransactionType = np.select(conditions, new_type)

    # get index prices
    fund_cf_bmark, index_prices = get_investment_sector_benchmark(fund_cf)
    return fund_cf_bmark, index_prices


def get_direct_alpha_rpt(
    df: pd.DataFrame,
    nav_df: pd.DataFrame,
    list_to_iterate: List[List[str]],
    _attributes_needed: List[str],
    _trailing_periods: dict,
):
    # bmark assignment is always at investment level
    fund_cf, index_prices = format_and_get_bmark_ks_pme(df)
    fund_df = (
        fund_cf[_attributes_needed + ["BenchmarkTicker"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    discount_df = self.get_alpha_discount_table(
        fund_df=fund_df, fund_cf=fund_cf, index_prices=index_prices
    )

    result = pd.DataFrame()
    for group_cols in list_to_iterate:
        for trailing_period in list(_trailing_periods.keys()):
            print(f"{group_cols} -  {trailing_period}")
            group_cols_date = group_cols.copy()
            group_cols_date.extend(["Date"])

            if trailing_period != "Incep":
                start_date = self._report_date + relativedelta(
                    months=(
                        self._trailing_periods.get(trailing_period) * -3
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
                    == PmeModel().nearest(index_prices.Date, start_date)
                ].iloc[0]

                index_end_value = index_prices[
                    index_prices.Date
                    == PmeModel().nearest(
                        index_prices.Date, self._report_date
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
                    self._attributes_needed + ["Portfolio"]
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
