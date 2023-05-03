from typing import List

import pandas as pd
import numpy as np
from gcm.inv.scenario import Scenario
from pyxirr import xirr
import datetime as dt
from dateutil.relativedelta import relativedelta
from pandas._libs.tslibs.offsets import MonthEnd
from functools import reduce

from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from .pme_model import PmeModel
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)


class RunTwrorIndustry(ReportingRunnerBase):
    def __init__(self):
        super().__init__(dao=Scenario.get_attribute("dao"))
        self._runner = Scenario.get_attribute("dao")
        self._report_date = Scenario.get_attribute("as_of_date")

    @property
    def _trailing_periods(self):
        return {
            "QTD": 1,
            "YTD": int(self._report_date.month / 3),
            "TTM": 4,
            "3Y": 12,
            "5Y": 20,
            "Incep": "Incep",
        }

    def append_deal_attributes(self, df):
        def my_dao_operation(dao, params):
            raw = (
                """select distinct os.Ticker OsTicker, h.ReportingName, hrpting.PredominantSector, d.*  from entitydata.OperationalSeries os
                    left join entitydata.Investment i on os.MasterId = i.OperationalSeriesId
                    left join entitydata.Deal d on i.DealId = d.MasterId
                    left join entitydata.Holding h on i.HoldingId = h.MasterId
                    left join entitydata.Holding hrpting on h.ReportingMasterId = hrpting.MasterId
                    where os.Ticker in """
                + self._os_series
            )
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        attrib = DaoRunner().execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=my_dao_operation,
        )
        # dup sectors for deals..
        attrib.PredominantSector = np.where(
            attrib.Name == "Epic Midstream",
            "Power Generation - Conventional",
            attrib.PredominantSector,
        )
        attrib.PredominantSector = np.where(
            attrib.Name == "Colossus",
            "FUNDS-Generalist",
            attrib.PredominantSector,
        )
        attrib.PredominantSector = np.where(
            attrib.Name == "Project Matrix",
            "FUNDS-Generalist",
            attrib.PredominantSector,
        )

        rslt = df.merge(
            attrib,
            how="left",
            left_on=["Owner", "Investment"],
            right_on=["OsTicker", "ReportingName"],
        )
        assert len(rslt) == len(df)
        return rslt

    def get_cf_ilevel(self, date):
        def my_dao_operation(dao, params):
            raw = f"select * from iLevel.vReportedCashFlows where OwnerName in {self._os_series} order by TransactionDate"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = DaoRunner().execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        df["TransactionType"] = np.where(
            df.TransactionType.str.contains("Contributions -"),
            "Contributions",
            df.TransactionType,
        )
        df["TransactionType"] = np.where(
            df.TransactionType.str.contains("Distributions -"),
            "Distributions",
            df.TransactionType,
        )
        df.rename(
            columns={"OwnerName": "Owner", "InvestmentName": "Investment"},
            inplace=True,
        )

        return df

    def get_all_usd_cash_flows(self):
        def my_dao_operation(dao, params):
            raw = (
                "select * from  [iLevel].[vExtendedCollapsedCashflows] where BaseCurrency='USD' and TransactionType in "
                "('Contributions - Investments and Expenses',"
                "'Distributions - Recallable',"
                "'Distributions - Return of Cost',"
                "'Distributions - Gain/(Loss)',"
                "'Distributions - Outside Interest',"
                "'Distributions - Dividends and Interest',"
                "'Contributions - Outside Expenses',"
                "'Contributions - Contra Contributions',"
                "'Contributions - Inside Expenses (DNAU)',"
                "'Distributions - Escrow Receivables', "
                "'Net Asset Value')"
            )
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = DaoRunner().execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        df["BaseAmount"] = np.where(
            df.TransactionType != "Net Asset Value",
            df.BaseAmount * -1,
            df.BaseAmount,
        )
        return df

    def get_cash_flows(self, date):
        def my_dao_operation(dao, params):
            raw = (
                "select * from  [iLevel].vExtendedCollapsedCashflows where OwnerName in "
                + self._os_series
                + " and "
                "TransactionDate <= '"
                + str(date)
                + "' and TransactionType in "
                "('Contributions - Investments and Expenses',"
                "'Distributions - Recallable',"
                "'Distributions - Return of Cost',"
                "'Distributions - Gain/(Loss)',"
                "'Distributions - Outside Interest',"
                "'Distributions - Dividends and Interest',"
                "'Contributions - Outside Expenses',"
                "'Contributions - Contra Contributions',"
                "'Contributions - Inside Expenses (DNAU)',"
                "'Distributions - Escrow Receivables', "
                "'Net Asset Value')"
            )
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = DaoRunner().execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        df.rename(
            columns={"OwnerName": "Owner", "InvestmentName": "Investment"},
            inplace=True,
        )
        df["BaseAmount"] = np.where(
            df.TransactionType != "Net Asset Value",
            df.BaseAmount * -1,
            df.BaseAmount,
        )
        return df

    def get_last_day_of_prior_quarter(self, p_date):
        return dt.date(
            p_date.year, 3 * ((p_date.month - 1) // 3) + 1, 1
        ) + dt.timedelta(days=-1)

    def get_last_day_of_the_quarter(self, p_date):
        quarter = (p_date.month - 1) // 3 + 1
        return dt.date(
            p_date.year + 3 * quarter // 12, 3 * quarter % 12 + 1, 1
        ) + dt.timedelta(days=-1)

    def calc_ctr(self, df):
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

    def fill_forward_navs(self, nav_orig):
        # fill forward NAVs
        # not used, but we could
        nav_filled = (
            nav_orig.pivot(
                index="TransactionDate",
                columns=["Owner", "Investment"],
                values="BaseAmount",
            )
            .ffill()
            .fillna(0)
        )
        nav_filled = nav_filled.T.reset_index().melt(
            id_vars=["Owner", "Investment"],
            value_vars=None,
            var_name="TransactionDate",
            value_name="BaseAmount",
        )
        nav_rslt = nav_filled.merge(
            nav_orig.drop(columns=["TransactionDate", "BaseAmount"]),
            how="left",
            left_on=["Owner", "Investment"],
            right_on=["Owner", "Investment"],
        )
        return nav_rslt

    def calc_tw_ror(self, df, group_cols, return_support_data=False):
        max_nav_date = (
            df[df.TransactionType == "Net Asset Value"]
            .groupby(["Owner", "Investment"])
            .TransactionDate.max()
            .reset_index()
            .rename(columns={"TransactionDate": "MaxNavDate"})
        )
        df = df.merge(
            max_nav_date,
            how="left",
            left_on=["Owner", "Investment"],
            right_on=["Owner", "Investment"],
        )
        df = df[df.TransactionDate <= df.MaxNavDate]

        df = df.sort_values("TransactionDate")
        df["lastq"] = df.TransactionDate.apply(
            lambda row: self.get_last_day_of_prior_quarter(row)
        )
        df["thisq"] = df.TransactionDate.apply(
            lambda row: self.get_last_day_of_the_quarter(row)
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
                self._attributes_needed
                + [
                    "Owner",
                    "Investment",
                    "PredominantStrategy",
                    "thisq",
                    "Nav",
                ]
            ].rename(columns={"Nav": "PriorNav", "thisq": "lastq"}),
            how="left",
            left_on=self._attributes_needed
            + ["Owner", "Investment", "PredominantStrategy", "lastq"],
            right_on=self._attributes_needed
            + ["Owner", "Investment", "PredominantStrategy", "lastq"],
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
        # data_sum = data_sum[data_sum.EndingNavAdj != 0]
        data_sum_lowest = data_sum_lowest[data_sum_lowest.PriorNavAdj > 0]
        # data_sum = data_sum[data_sum.PriorNav > 50000]
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
        # uncomment for original results
        # data_sum['EndingNavAdj'] = data_sum.Nav + data_sum.BaseAmount
        # data_sum['PriorNavAdj'] = data_sum.PriorNav - data_sum.wAmount
        # data_sum = data_sum.fillna(0)
        # # data_sum = data_sum[data_sum.EndingNavAdj != 0]
        # data_sum = data_sum[data_sum.PriorNavAdj > 0]
        # # data_sum = data_sum[data_sum.PriorNav > 50000]
        # data_sum = data_sum[data_sum.Nav > 0]
        # data_sum = data_sum[data_sum.PriorNav > 0]

        data_sum["Ror"] = (
            data_sum.EndingNavAdj - data_sum.PriorNav
        ) / data_sum.PriorNavAdj
        data_sum["Gain"] = data_sum.EndingNavAdj - data_sum.PriorNav

        # data_sum.to_csv('C:/Tmp/check RORs.csv')

        # data_sum.pivot(index='thisq', columns='Investment', values='Ror').to_csv('C:/Tmp/rors pivot.csv')
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

    def calc_irr(self, cf, group_cols=None, type="Gross"):
        # all funds/deals in cfs dataframe are what the result will reflect (i.e. do filtering beforehand)
        if len(cf) == 0:
            return pd.DataFrame(columns=["Name", type + "Irr"])
        if group_cols is None:
            irr = xirr(
                cf[["TransactionDate", "BaseAmount"]]
                .groupby("TransactionDate")
                .sum()
                .reset_index()
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

    def ann_return(
        self, return_df, trailing_periods, freq=4, return_NoObs=True
    ):
        trailing_periods_obs = [
            self._trailing_periods[x] for x in trailing_periods
        ]
        result = pd.DataFrame()
        for i in return_df.columns:
            returns = return_df[[i]]
            returns = returns.dropna()
            if (
                len(trailing_periods_obs) == 1
                and trailing_periods_obs[0] != "Incep"
                and len(returns) < trailing_periods_obs[0]
            ):
                ann_return = pd.DataFrame(
                    {
                        returns.columns.name: [None],
                        "AnnRor": [None],
                        "NoObs": [None],
                    }
                )
                if return_NoObs:
                    result = pd.concat([result, ann_return])
                else:
                    ann_return.drop(columns=["NoObs"])
                    result = pd.concat([result, ann_return])

            for period in trailing_periods:
                trailing_period = self._trailing_periods.get(period)
                if trailing_period != "Incep":
                    if len(returns) < trailing_period:
                        continue
                    else:
                        start_date = self._report_date + relativedelta(
                            months=-3 * (trailing_period), days=1
                        )
                        return_sub = returns.loc[
                            start_date : self._report_date
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

    def calc_multiple(self, cf, group_cols=None, type="Gross"):
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

    def get_commitment(self, date, convert_to_usd):
        def my_dao_operation(dao, params):
            raw = (
                "select * from  [iLevel].[vExtendedCollapsedCashflows] where OwnerName in "
                + self._os_series
                + " and "
                "TransactionDate <= '"
                + str(date)
                + "' and TransactionType in "
                "('Contributions - Investments and Expenses',"
                "'Distributions - Recallable',"
                "'Contributions - Contra Contributions',"
                "'Contributions - Outside Expenses (AU)',"
                "'Unfunded Commitment Without Modification',"
                "'Local Discounted Commitments (For USD Holdings in Foreign Portfolios)')"
            )
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = DaoRunner().execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        if convert_to_usd:
            df = self.convert_amt_to_usd(df)

        df.rename(
            columns={
                "OwnerName": "Owner",
                "InvestmentName": "Investment",
                "BaseAmount": "Commitment",
            },
            inplace=True,
        )
        unfunded = df[
            (
                df.TransactionType
                == "Unfunded Commitment Without Modification"
            )
            & (df.TransactionDate == date)
        ]
        funded = df[
            df.TransactionType
            != "Unfunded Commitment Without Modification"
        ]
        funded = (
            funded[["Owner", "Investment", "Commitment"]]
            .groupby(["Owner", "Investment"])
            .sum()
            .reset_index()
        )

        rslt = pd.concat(
            [unfunded[["Owner", "Investment", "Commitment"]], funded]
        )
        rslt = rslt.groupby(["Owner", "Investment"]).sum().reset_index()

        return rslt

    def get_ctrs(self, ctrs, trailing_periods):
        def safe_division(numerator, denominator):
            """Return 0 if denominator is 0."""
            return denominator and numerator / denominator

        result = pd.DataFrame()
        for trailing_period in trailing_periods:
            if trailing_period != "Incep":
                start_date = self._report_date + relativedelta(
                    months=-3
                    * (self._trailing_periods.get(trailing_period)),
                    days=1,
                )
                ctr = self.calc_ctr(
                    ctrs.loc[start_date : self._report_date]
                )
                # if trailing_period not in ['QTD', 'YTD', 'TTM']:
                # ctr.Ctr = (1 + ctr['Ctr']) ** (4 / ctr.NoObs) - 1
                # ctr.NoObs = np.where(ctr.NoObs == 0, 1, ctr.NoObs)
                # ctr.AnnCtr = (1 + ctr.Ctr) ** (4 / ctr.NoObs) - 1
                ctr.AnnCtr = ctr[["Ctr", "NoObs"]].apply(
                    lambda row: (1 + row["Ctr"])
                    ** (safe_division(4, row["NoObs"]))
                    - 1,
                    axis=1,
                )
                ctr.Ctr = np.where(ctr.NoObs < 5, ctr.Ctr, ctr.AnnCtr)
            else:
                ctr = self.calc_ctr(ctrs)
                # ctr.Ctr = (1 + ctr['Ctr']) ** (4 / ctr.NoObs) - 1
                # ctr.NoObs = np.where(ctr.NoObs == 0, 1, ctr.NoObs)
                # ctr.Ctr = np.where(ctr.NoObs < 5, ctr.Ctr, (1 + ctr.Ctr) ** (4 / ctr.NoObs) - 1)

                # ctr.AnnCtr = (1 + ctr.Ctr) ** (4 / ctr.NoObs) - 1
                ctr.AnnCtr = ctr[["Ctr", "NoObs"]].apply(
                    lambda row: (1 + row["Ctr"])
                    ** (safe_division(4, row["NoObs"]))
                    - 1,
                    axis=1,
                )
                ctr.Ctr = np.where(ctr.NoObs < 5, ctr.Ctr, ctr.AnnCtr)
            ctr["Period"] = trailing_period
            result = pd.concat([result, ctr])
        return result

    def get_ror_ctr(self, df, group_cols, trailing_periods):
        rors = self.calc_tw_ror(
            df, group_cols=group_cols, return_support_data=True
        )
        if len(rors) == 0:
            return pd.DataFrame(
                columns=["Name", "Period", "AnnRor", "Ctr"]
            )

        try:
            rors.to_csv(
                f"C:/Tmp/ror data check {str(group_cols)} {str(self._portfolio)} {str(self._report_date)}.csv"
            )
        except:
            print("Cannot write file")

        ann_ror = self.ann_return(
            rors.pivot_table(index="Date", columns="Name", values="Ror"),
            trailing_periods=trailing_periods,
            freq=4,
        )

        ctr = self.get_ctrs(
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
        # result.Ctr = np.where(result.NoObs_x < 5, result.Ctr, (1 + result.Ctr) ** (4 / result.NoObs_x) - 1)
        result["group_cols"] = str(group_cols)
        return result

    def get_irr_df_rpt(self, df, list_to_iterate):
        irr_data = pd.concat(
            [self.calc_irr(df, group_cols=i) for i in list_to_iterate]
        )[["Name", "GrossIrr"]]
        irr_data["NoObs"] = "Incep"

        return irr_data

    def get_ror_ctr_df_rpt(self, df, trailing_periods, list_to_iterate):
        ror_ctr_df = pd.concat(
            [
                self.get_ror_ctr(
                    df, group_cols=i, trailing_periods=trailing_periods
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
                    sub.Name == self._portfolio_ticker
                ].AnnRor.squeeze() / sum(subb[~subb.Ctr.isnull()].Ctr)
                subb["Ctr"] = subb.Ctr * ctr_total.squeeze()
                result = pd.concat([result, subb])[
                    ["Name", "AnnRor", "Ctr", "Period"]
                ]
        return result

    def get_multiple_df_rpt(self, df, list_to_iterate):
        # if rmv:
        #     df = df[df.TransactionDate <= df.MaxNavDate]
        # else:
        #     df.loc[1, 'BaseAmount'] = df[df.TransactionDate > df.MaxNavDate][['Owner', 'Investment', 'BaseAmount']].\
        #         groupby(['Owner', 'Investment']).sum().reset_index()

        multiple_df = pd.concat(
            [self.calc_multiple(df, group_cols=i) for i in list_to_iterate]
        )[["Name", "GrossMultiple"]]
        multiple_df["NoObs"] = "Incep"
        return multiple_df

    def calc_sum(self, df, group_cols):
        rslt = df.groupby(group_cols).sum().reset_index()
        rslt["Name"] = rslt.apply(
            lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
        )
        return rslt

    def calc_max_or_min(self, df, group_cols, min=True):
        if min:
            rslt = df.groupby(group_cols).min().reset_index()
        else:
            rslt = df.groupby(group_cols).max().reset_index()

        rslt["Name"] = rslt.apply(
            lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
        )
        return rslt

    def calc_wtd_holding_period(self, df, group_cols):
        df["PctEquity"] = df.BaseAmount / df.groupby(
            group_cols
        ).BaseAmount.transform("sum")
        df["wHoldingPeriod"] = df.PctEquity * df.HoldingPeriod

        rslt = df.groupby(group_cols).wHoldingPeriod.sum().reset_index()
        rslt["Name"] = rslt.apply(
            lambda x: "_".join([str(x[i]) for i in group_cols]), axis=1
        )
        return rslt

    def recurse_down(
        self,
        df: pd.DataFrame,
        group_by_list: List[str],
        depth: int,
        atomic_units: List[str],
    ) -> pd.DataFrame:

        # attrib = df[group_by_list].drop_duplicates()
        final_df_cache = []
        # final_df_cache = pd.DataFrame()
        if df.shape[0] > 0:
            current_grouping_struct = df.groupby(group_by_list[depth])
            for name, group in current_grouping_struct:
                print(name)
                print(group)
                filtered_cfs = df.copy()
                # for i in atomic_units:
                #     filter_item = list(set(group[i].to_list()))
                #     filtered_cfs = filtered_cfs[filtered_cfs[i].isin(filter_item)]
                simple_frame = pd.DataFrame(
                    {
                        "Name": [name],
                        "Layer": [depth],
                        "GrossIrr": [self.calc_irr(group)],
                    }
                )
                # final_df_cache = pd.concat([simple_frame, final_df_cache])
                final_df_cache.append(simple_frame)

                assert type(group) is pd.DataFrame
                reset = depth + 1
                if group.shape[0] > 0 and depth < len(group_by_list) - 1:
                    children_data_frame = self.recurse_down(
                        df=group,
                        group_by_list=group_by_list,
                        depth=reset,
                        atomic_units=atomic_units,
                    )
                    if children_data_frame is not None:
                        final_df_cache.append(children_data_frame)
        if len(final_df_cache) > 0:
            return pd.concat(final_df_cache)
        return None

    def recurse_down_order(
        self,
        df: pd.DataFrame,
        group_by_list: List[str],
        depth: int,
        counter: int,
    ) -> pd.DataFrame:
        # if counter == 0:
        #     counter = 0
        #     last_depth = 0
        # else:
        #     last_depth = depth - 1
        #     counter = counter if counter counter - 1
        #     last_depth = depth - 1

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
                    children_data_frame, counter = self.recurse_down_order(
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

    def calc_duration(self, discount_df, group_cols):
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

    def get_max_or_min_df_rpt(self, df, list_to_iterate, min=True):
        rslt = pd.concat(
            [
                self.calc_max_or_min(df, group_cols=i, min=min)
                for i in list_to_iterate
            ]
        )

        # used for big join... remove
        rslt["NoObs"] = "Incep"
        return rslt

    def get_holding_periods_rpt(self, df, discount_df, list_to_iterate):
        max_nav_date = (
            df[df.TransactionType == "Net Asset Value"]
            .groupby(["Name"])
            .TransactionDate.max()
            .reset_index()
            .rename(columns={"TransactionDate": "MaxNavDate"})
        )

        min_cf_date = (
            df[self._attributes_needed + ["TransactionDate", "Portfolio"]]
            .groupby(self._attributes_needed + ["Portfolio"])
            .min()
            .reset_index()
        )
        date_df = min_cf_date.merge(max_nav_date, how="outer")
        date_df["HoldingPeriod"] = (
            date_df.MaxNavDate - date_df.TransactionDate
        ) / pd.Timedelta("365 days")

        equity_invested = (
            df[df.TransactionType == "Contribution"]
            .groupby(self._attributes_needed + ["Portfolio"])
            .BaseAmount.sum()
            .abs()
            .reset_index()
        )

        holding_period_df = date_df.merge(
            equity_invested[["Name", "BaseAmount"]],
            how="left",
            left_on="Name",
            right_on="Name",
        )

        rslt = pd.concat(
            [
                self.calc_duration(discount_df, group_cols=i)
                for i in list_to_iterate
            ]
        )
        # rslt = pd.concat([self.calc_wtd_holding_period(holding_period_df, group_cols=i) for i in list_to_iterate])
        rslt = rslt[["Name", "Duration"]]

        # used for big join... remove
        rslt["NoObs"] = "Incep"
        return rslt, max_nav_date

    def get_sum_df_rpt(self, df, list_to_iterate):
        sum_df = pd.concat(
            [self.calc_sum(df, group_cols=i) for i in list_to_iterate]
        )
        sum_df["NoObs"] = "Incep"
        return sum_df

    # def get_adj_nav(self, owner, date):

    def get_investment_sector_benchmark(self, df):
        # benchmark_map = pd.read_csv('C:/Tmp/TickerPmeMap.csv')
        # df_bmark_mapped = df.merge(benchmark_map.rename(columns={'Ticker': 'BenchmarkTicker'}), how='left',
        #                 left_on='PredominantSector',
        #                 right_on='PredominantSector')
        # df_bmark_mapped.BenchmarkTicker = np.where(df_bmark_mapped.BenchmarkTicker.isnull(),
        #                                            'SPXT Index',
        #                                            df_bmark_mapped.BenchmarkTicker)
        # hardcode spxt per amy, above will do industry specific bmarks
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

        index_prices = DaoRunner().execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        index_prices_rslt = index_prices.pivot(
            index="Date", columns="Ticker", values="PxLast"
        ).reset_index()

        return df_bmark_mapped, index_prices_rslt

    def get_pct_gain_rpt(self, df, list_to_iterate):
        # add if we want
        dollar_gain = (
            df[
                df.TransactionType.isin(
                    ["Distributions", "Net Asset Value"]
                )
            ]
            .groupby(
                self._attributes_needed + ["TransactionType", "Portfolio"]
            )
            .BaseAmount.sum()
            .reset_index()
        )
        tmp = self.get_sum_df_rpt(dollar_gain, list_to_iterate)

    def format_and_get_bmark_ks_pme(self, df):
        # prep data
        fund_cf = df[
            self._attributes_needed
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
        fund_cf_bmark, index_prices = self.get_investment_sector_benchmark(
            fund_cf
        )
        return fund_cf_bmark, index_prices

    def get_fv_cashflow_df(self, fund_df, fund_cf, index_prices):
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

            fv_cashflows = PmeModel().KS_PME(
                dates_cashflows=single_fund_group_sum["TransactionDate"],
                cashflows=single_fund_group_sum["BaseAmount"],
                cashflows_type=single_fund_group_sum["TransactionType"],
                dates_index=fund_specific_index.iloc[:, 0],
                index=fund_specific_index.iloc[:, 1],
                auto_NAV=False,
            )
            index_value = fund_specific_index[
                fund_specific_index.Date
                == PmeModel().nearest(
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

    def get_ks_pme_rpt(self, df, nav_df, list_to_iterate):
        # bmark assignment is always at investment level
        fund_cf, index_prices = self.format_and_get_bmark_ks_pme(df)
        fund_df = (
            fund_cf[self._attributes_needed + ["BenchmarkTicker"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        result = pd.DataFrame()
        for trailing_period in list(self._trailing_periods.keys()):
            print(trailing_period)
            if trailing_period in ["YTD", "QTD", "TTM"]:
                continue
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
                # starting_investment = starting_investment.groupby(group_cols).BaseAmount.sum().reset_index()
                starting_investment.BaseAmount = (
                    starting_investment.BaseAmount * -1
                )
                starting_investment["Date"] = pd.to_datetime(start_date)
                starting_investment["TransactionType"] = "Capital Call"

                fund_cf_filtered = fund_cf[
                    fund_cf.TransactionDate >= pd.to_datetime(start_date)
                ]
                fund_cf_filtered = pd.concat(
                    [starting_investment, fund_cf_filtered]
                )
            else:
                fund_cf_filtered = fund_cf
                starting_investment = None

            fv_cashflows_df = self.get_fv_cashflow_df(
                fund_df=fund_df,
                fund_cf=fund_cf_filtered,
                index_prices=index_prices,
            )
            if len(fv_cashflows_df) == 0:
                continue
            fv_cashflows_df_with_attrib = fv_cashflows_df.merge(
                df[
                    self._attributes_needed + ["Portfolio"]
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

    def get_alpha_discount_table(self, fund_df, fund_cf, index_prices):
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

            discount_table = PmeModel().discount_table(
                dates_cashflows=single_fund_group_sum["TransactionDate"],
                cashflows=single_fund_group_sum["BaseAmount"],
                cashflows_type=single_fund_group_sum["TransactionType"],
                dates_index=fund_specific_index.iloc[:, 0],
                index=fund_specific_index.iloc[:, 1],
            )
            discount_table["Name"] = fund_df.Name[idx]

            # def time_to_int(dateobj):
            #     total = int(dateobj.strftime('%S'))
            #     total += int(dateobj.strftime('%M')) * 60
            #     total += int(dateobj.strftime('%H')) * 60 * 60
            #     total += (int(dateobj.strftime('%j')) - 1) * 60 * 60 * 24
            #     total += (int(dateobj.strftime('%Y')) - 1970) * 60 * 60 * 24 * 365
            #     return total
            # def
            #
            # distrib = discount_table[discount_table.Type.isin(['Distribution', 'Value'])].reset_index(drop=True)
            # contrib = discount_table[discount_table.Type.isin(['Capital Call'])].reset_index(drop=True)
            # # distrib.Date = pd.to_datetime(distrib.Date)
            # # contrib.Date = pd.to_datetime(contrib.Date)
            #
            # distrib['DateInt'] = distrib.Date.apply(time_to_int)
            # contrib['DateInt'] = contrib.Date.apply(time_to_int)
            #
            # distrib['date_weight'] = distrib.Date * distrib.Discounted

            discount_table_rslt = pd.concat(
                [discount_table_rslt, discount_table]
            )
        return discount_table_rslt

    def get_direct_alpha_rpt(self, df, nav_df, list_to_iterate):
        # bmark assignment is always at investment level
        fund_cf, index_prices = self.format_and_get_bmark_ks_pme(df)
        fund_df = (
            fund_cf[self._attributes_needed + ["BenchmarkTicker"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        discount_df = self.get_alpha_discount_table(
            fund_df=fund_df, fund_cf=fund_cf, index_prices=index_prices
        )

        result = pd.DataFrame()
        for group_cols in list_to_iterate:
            for trailing_period in list(self._trailing_periods.keys()):
                print(f"{group_cols} -  {trailing_period}")
                group_cols_date = group_cols.copy()
                group_cols_date.extend(["Date"])

                if trailing_period != "Incep":
                    start_date = self._report_date + relativedelta(
                        months=(
                            self._trailing_periods.get(trailing_period)
                            * -3
                        ),
                        days=1,
                    )
                    starting_investment = nav_df[
                        nav_df.TransactionDate
                        == pd.to_datetime(
                            start_date + relativedelta(days=-1)
                        )
                    ]
                    if len(starting_investment) == 0:
                        continue

                    # starting_investment = starting_investment.groupby(group_cols).BaseAmount.sum().reset_index()
                    starting_investment.BaseAmount = (
                        starting_investment.BaseAmount * -1
                    )
                    starting_investment["Date"] = pd.to_datetime(
                        start_date
                    )

                    index_start_value = index_prices[
                        index_prices.Date
                        == PmeModel().nearest(
                            index_prices.Date, start_date
                        )
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
                assert len(grouped_filtered) == len(
                    discount_df_with_attrib
                )

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

    def get_to_usd_fx_rates(self):
        def my_dao_operation(dao, params):
            raw = """select AsOfDt Date, FromCurrcyCd, ToCurrcyCd, Multiplier from analyticsdata.FXFact
                    where ToCurrcyCd = 'USD'
                    and MonthsForward = 0
                    order by FromCurrcyCd, AsOfDt"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        fx_rates = DaoRunner().execute(
            params={},
            source=DaoSource.PubDwh,
            operation=my_dao_operation,
        )
        return fx_rates

    def convert_amt_to_usd(self, df):
        fx_rates = self.get_to_usd_fx_rates()
        assert len(
            df[
                (df.TransactionDate.isin(fx_rates.Date))
                & (df.BaseCurrency.isin(fx_rates.FromCurrcyCd))
            ]
        ) == len(df[df.BaseCurrency != "USD"])

        df_fx = df.merge(
            fx_rates,
            how="left",
            left_on=["BaseCurrency", "TransactionDate"],
            right_on=["FromCurrcyCd", "Date"],
        )
        assert len(df) == len(df_fx)
        df_fx.Multiplier = np.where(
            df_fx.Multiplier.isnull(), 1, df_fx.Multiplier
        )
        df_fx["BaseAmount"] = df_fx.BaseAmount * df_fx.Multiplier

        result = df_fx[df.columns]
        return result

    def pivot_trailing_period_df(self, df):
        df_melted = pd.melt(df, id_vars=["Name", "Period"])
        df_melted = df_melted.pivot_table(
            index="Name", columns=["Period", "variable"], values=["value"]
        ).reset_index()
        return df_melted

    def get_horizon_irr_df_rpt(self, df, nav_df, list_to_iterate):
        result = pd.DataFrame()
        for trailing_period in list(self._trailing_periods.keys()):
            print(trailing_period)
            if trailing_period in ["YTD", "QTD", "TTM"]:
                continue
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

                irr_data = self.calc_irr(
                    fund_cf_filtered, group_cols=group_cols
                )[["Name", "GrossIrr"]]
                irr_data["NoObs"] = trailing_period
                result = pd.concat([result, irr_data])

        return result

    def get_horizon_tvpi_df_rpt(self, df, nav_df, list_to_iterate):
        result = pd.DataFrame()
        for trailing_period in list(self._trailing_periods.keys()):
            print(trailing_period)
            if trailing_period in ["YTD", "QTD", "TTM"]:
                continue
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

                multiple_df = self.calc_multiple(
                    fund_cf_filtered, group_cols=group_cols
                )[["Name", "GrossMultiple"]]
                multiple_df["NoObs"] = trailing_period

                result = pd.concat([result, multiple_df])

        return result

    def get_burgiss_bmark(self):
        def my_dao_operation(dao, params):
            # hardcoding params
            raw = """
                    select Measure, Pooled, BottomQuartile, Median, TopQuartile from burgiss.BenchmarkFact
                    where Date = '2022-9-30'
                    and AssetGroup='Buyout'
                    and GeographyGroup='All'
                    and Vintage = 'All'
                    and Measure in (
                    'Direct Alpha - S&P 500 (TR) - 5 Year',
                    'Direct Alpha - S&P 500 (TR) - 3 Year',
                    'PME - S&P 500 (TR) - 5 Year',
                    'PME - S&P 500 (TR) - 3 Year',
                    'IRR - 3 Year',
                    'IRR - 5 Year',
                    'PME - S&P 500 (TR)',
                    'Direct Alpha - S&P 500 (TR)',
                    'IRR', 'TVPI',
                    'TWR - QTD', 'TWR - 1 Year', 'TWR - 3 Year', 'TWR - 5 Year',
                    'TWR - ITD')"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = DaoRunner().execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    {
                        "Measure": [
                            "TVPI - 5 Year",
                            "TVPI - 3 Year",
                            "CTR - ITD",
                            "CTR - 5 Year",
                            "CTR - 3 Year",
                            "CTR - 1 Year",
                            "CTR - QTD",
                        ]
                    }
                ),
            ]
        )
        rslt = df.set_index("Measure").T.reindex(
            ["Pooled", "TopQuartile", "Median", "BottomQuartile"]
        )[
            [
                "PME - S&P 500 (TR)",
                "Direct Alpha - S&P 500 (TR)",
                "TVPI",
                "IRR",
                "TWR - ITD",
                "CTR - ITD",
                "PME - S&P 500 (TR) - 5 Year",
                "Direct Alpha - S&P 500 (TR) - 5 Year",
                "TVPI - 5 Year",
                "IRR - 5 Year",
                "TWR - 5 Year",
                "CTR - 5 Year",
                "PME - S&P 500 (TR) - 3 Year",
                "Direct Alpha - S&P 500 (TR) - 3 Year",
                "TVPI - 3 Year",
                "IRR - 3 Year",
                "TWR - 3 Year",
                "CTR - 3 Year",
                "TWR - 1 Year",
                "CTR - 1 Year",
                "TWR - QTD",
                "CTR - QTD",
            ]
        ]
        rslt[rslt.columns[rslt.columns.str.contains("IRR")]] = (
            rslt[rslt.columns[rslt.columns.str.contains("IRR")]] / 100
        )
        rslt[rslt.columns[rslt.columns.str.contains("Alpha")]] = (
            rslt[rslt.columns[rslt.columns.str.contains("Alpha")]] / 100
        )
        return rslt

    def get_portfolio_os_investment_type(self, investment_type):
        def my_dao_operation(dao, params):
            raw = f"""select distinct OperationalSeriesTicker, PortfolioReportingName
             from analytics.MasterEntityDataPortfolioPortfolioSeriesOperationalSeries
                where OperationalSeriesInvestmentType = '{str(investment_type)}'"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        portfolios = DaoRunner().execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=my_dao_operation,
        )
        return portfolios

    def get_portfolios_pe_only(self):
        def my_dao_operation(dao, params):
            raw = """
                    SELECT DISTINCT [Portfolio Ticker] PortfolioTicker, [Portfolio Reporting Name] PortfolioReportingName, 
                    [Portfolio Currency] PortfolioCurrency,
                    [Operational Series Ticker] OsTicker, 
                    [Operational Series Name] OsName, 
                    [Operational Series Predominant Asset Class] OsAssetClass,
                    [Holding Reporting Name] HoldingName, 
                    [Holding Currency] HoldingCurrency,
                    [Deal Name] DealName,
                    [Deal Vintage Year] DealVintage, 
                    [Deal Predominant Asset Class] DealAssetClass,
                    FROM [analytics].[MasterEntityDataInvestmentTrack]
                    order by [Portfolio Reporting Name], [Holding Reporting Name]"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        portfolios = DaoRunner().execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=my_dao_operation,
        )
        return portfolios

    def get_portfolio_os(self):
        def my_dao_operation(dao, params):
            raw = """select distinct PortfolioMasterId, OperationalSeriesTicker, PortfolioReportingName, OperationalSeriesInvestmentType, 
                 PortfolioTicker, PortfolioCurrency 
             from analytics.MasterEntityDataPortfolioPortfolioSeriesOperationalSeries"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        portfolios = DaoRunner().execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=my_dao_operation,
        )
        return portfolios

    def get_pe_only_portfolios(self):
        def my_dao_operation(dao, params):
            raw = """
                select distinct [Portfolio Master Id] PortfolioMasterId, [Operational Series Ticker] OperationalSeriesTicker, 
                [Portfolio Reporting Name] PortfolioReportingName, [Operational Series Investment Type] OperationalSeriesInvestmentType, 
                 [Portfolio Ticker] PortfolioTicker, [Portfolio Currency] PortfolioCurrency, [Deal Predominant Asset Class]  DealPredominantAssetClass
             from analytics.MasterEntityDataInvestmentTrack"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        portfolios = DaoRunner().execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=my_dao_operation,
        )
        not_pe = portfolios[
            portfolios.DealPredominantAssetClass != "Private Equity"
        ].PortfolioReportingName
        result = (
            portfolios[~portfolios.PortfolioReportingName.isin(not_pe)]
            .drop(columns="DealPredominantAssetClass")
            .drop_duplicates()
        )
        return result

    def get_manager_investments(self):
        def my_dao_operation(dao, params):
            raw = """
                    SELECT DISTINCT [Portfolio Ticker] PortfolioTicker, [Portfolio Reporting Name] PortfolioName, 
                    [Portfolio Currency] PortfolioCurrency,
                    [Operational Series Ticker] OsTicker, 
                    [Operational Series Name] OsName, 
                    [Operational Series Predominant Asset Class] OsAssetClass,
                    [Holding Reporting Name] HoldingName, 
                    [Holding Currency] HoldingCurrency,
                    [Deal Name] DealName,
                    [Deal Vintage Year] DealVintage, 
                    [Investment Realization Type] Realizationtype,
                    [Investment Manager Legal Name] InvestmentManagerName,
                    [Investment Manager Master Id] InvestmentManagerId
                    FROM [analytics].[MasterEntityDataInvestmentTrack]
                    where [Investment Manager Legal Name] is not NULL
                    order by [Portfolio Reporting Name], [Holding Reporting Name]"""
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        portfolios = DaoRunner().execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=my_dao_operation,
        )
        return portfolios

    def get_twror_by_industry_rpt(
        self,
        owner,
        trailing_periods,
        list_to_iterate,
        convert_to_usd=False,
    ):
        if self._mgr_df is not None:
            self._mgr_df = self._mgr_df[
                self._mgr_df.HoldingName
                != "Peak Rock Capital Credit Fund II LP"
            ]
            holdings_filter = (
                self._mgr_df[self._mgr_df.InvestmentManagerName == owner]
                .HoldingName.drop_duplicates()
                .to_list()
            )
            holdings_filter = [
                x for x in holdings_filter if "Credit" not in x
            ]
        else:
            holdings_filter = None

        cf_irr = self.get_cf_ilevel(self._report_date)
        cf_irr = self.append_deal_attributes(cf_irr)
        cf_irr["Portfolio"] = owner

        if holdings_filter is not None:
            cf_irr = cf_irr[cf_irr.ReportingName.isin(holdings_filter)]

        if self._type_filter is not None:
            cf_irr = cf_irr[
                cf_irr.PredominantInvestmentType.isin(self._type_filter)
            ]
        if self._asset_class_filter is not None:
            cf_irr = cf_irr[
                cf_irr.PredominantAssetClass.isin(self._asset_class_filter)
            ]
        # cf_irr = cf_irr[cf_irr.PredominantInvestmentType == 'Primary Fund']
        if convert_to_usd:
            cf_irr = self.convert_amt_to_usd(cf_irr)

        df = self.get_cash_flows(self._report_date)
        df = self.append_deal_attributes(df)
        df["Portfolio"] = owner

        if holdings_filter is not None:
            df = df[df.ReportingName.isin(holdings_filter)]

        if self._type_filter is not None:
            df = df[df.PredominantInvestmentType.isin(self._type_filter)]
        if self._asset_class_filter is not None:
            df = df[
                df.PredominantAssetClass.isin(self._asset_class_filter)
            ]
        # df = df[df.PredominantInvestmentType == 'Primary Fund']
        if convert_to_usd:
            df = self.convert_amt_to_usd(df)

        nav_df = df[df.TransactionType == "Net Asset Value"]
        nav_df["Portfolio"] = owner

        if (
            len(
                cf_irr[
                    (cf_irr.TransactionType == "Net Asset Value")
                    & (cf_irr.TransactionDate == self._report_date)
                ]
            )
            == 0
        ):
            print("No Current NAV")
            return

        direct_alpha, discount_df = self.get_direct_alpha_rpt(
            df=cf_irr, nav_df=nav_df, list_to_iterate=list_to_iterate
        )
        ks_pme = self.get_ks_pme_rpt(
            df=cf_irr, nav_df=nav_df, list_to_iterate=list_to_iterate
        )

        # TODO: make trailing_periods dynamic on YTD/QTD/TTM etc
        ror_ctr_df = self.get_ror_ctr_df_rpt(
            df,
            list_to_iterate=list_to_iterate,
            trailing_periods=trailing_periods,
        )

        # irr_data = self.get_irr_df_rpt(cf_irr, list_to_iterate=list_to_iterate)
        # multiple_data = self.get_multiple_df_rpt(cf_irr, list_to_iterate=list_to_iterate)

        horizon_irr = self.get_horizon_irr_df_rpt(
            df=cf_irr, nav_df=nav_df, list_to_iterate=list_to_iterate
        )
        horizon_multiple = self.get_horizon_tvpi_df_rpt(
            df=cf_irr, nav_df=nav_df, list_to_iterate=list_to_iterate
        )

        commitment = self.get_commitment(self._report_date, convert_to_usd)
        commitment = self.append_deal_attributes(commitment)
        commitment["Portfolio"] = owner

        if holdings_filter is not None:
            commitment = commitment[
                commitment.ReportingName.isin(holdings_filter)
            ]
        if self._type_filter is not None:
            commitment = commitment[
                commitment.PredominantInvestmentType.isin(
                    self._type_filter
                )
            ]
        if self._asset_class_filter is not None:
            commitment = commitment[
                commitment.PredominantAssetClass.isin(
                    self._asset_class_filter
                )
            ]
        # commitment = commitment[commitment.PredominantInvestmentType == 'Primary Fund']

        commitment_df = self.get_sum_df_rpt(commitment, list_to_iterate)[
            ["Name", "Commitment", "NoObs"]
        ]

        nav = cf_irr[cf_irr.TransactionType == "Net Asset Value"].rename(
            columns={"BaseAmount": "Nav"}
        )
        nav_df = self.get_sum_df_rpt(nav, list_to_iterate)[
            ["Name", "Nav", "NoObs"]
        ]

        discount_df_with_attrib = discount_df[
            ["Name", "Date", "Discounted", "Type"]
        ].merge(
            df[self._attributes_needed + ["Portfolio"]].drop_duplicates(),
            how="left",
            left_on="Name",
            right_on="Name",
        )
        assert len(discount_df) == len(discount_df_with_attrib)

        holding_period_df, max_nav_date = self.get_holding_periods_rpt(
            cf_irr, discount_df_with_attrib, list_to_iterate
        )

        ror_ctr_melted = self.pivot_trailing_period_df(ror_ctr_df)
        ks_pme_melted = self.pivot_trailing_period_df(ks_pme)
        direct_alpha_melted = self.pivot_trailing_period_df(direct_alpha)
        irr_melted = self.pivot_trailing_period_df(
            horizon_irr.rename(columns={"NoObs": "Period"})
        )
        multiple_melted = self.pivot_trailing_period_df(
            horizon_multiple.rename(columns={"NoObs": "Period"})
        )

        # report specific formatting
        attrib = df[self._attributes_needed].drop_duplicates()
        attrib["Portfolio"] = self._portfolio_ticker

        for group in range(0, len(list_to_iterate)):
            attrib["Group" + str(group)] = attrib.apply(
                lambda x: "_".join(
                    [str(x[i]) for i in list_to_iterate[group]]
                ),
                axis=1,
            )
        attrib = attrib.sort_values(
            [
                col_name
                for col_name in attrib.columns[
                    attrib.columns.str.contains("Group")
                ]
            ]
        )

        if "PredominantSector" in attrib.columns:
            attrib.PredominantSector = (
                attrib.PredominantSector.str.replace("FUNDS-", "")
            )
            attrib.PredominantSector = (
                attrib.PredominantSector.str.replace("COS-", "")
            )
        if "PredominantRealizationTypeCategory" in attrib.columns:
            attrib.PredominantRealizationTypeCategory = (
                attrib.PredominantRealizationTypeCategory.str.replace(
                    "Realized & Partially/Substantially Realized",
                    "Realized & Partial",
                )
            )
            attrib.PredominantRealizationTypeCategory = np.where(
                attrib.PredominantRealizationTypeCategory.isnull(),
                "Not Tagged",
                attrib.PredominantRealizationTypeCategory,
            )

        if "PredominantInvestmentType" in attrib.columns:
            attrib["Order"] = np.select(
                [
                    (attrib.PredominantInvestmentType == "Primary Fund"),
                    (attrib.PredominantInvestmentType == "Secondary"),
                    (
                        attrib.PredominantInvestmentType
                        == "Co-investment/Direct"
                    ),
                ],
                [1, 2, 3],
            )
            attrib = attrib.sort_values("Order")

        ordered_recursion = [
            item for sublist in list_to_iterate for item in sublist
        ]
        ordered_recursion = [
            ordered_recursion[i]
            for i in range(len(ordered_recursion))
            if i == ordered_recursion.index(ordered_recursion[i])
        ]
        ordered_rpt_items, counter_df = self.recurse_down_order(
            attrib, group_by_list=ordered_recursion, depth=0, counter=0
        )
        rslt = reduce(
            lambda left, right: pd.merge(
                left, right, on=["Name"], how="outer"
            ),
            [
                ordered_rpt_items,
                commitment_df,
                holding_period_df,
                max_nav_date,
                nav_df,
                irr_melted,
                multiple_melted,
                ks_pme_melted,
                direct_alpha_melted,
                ror_ctr_melted,
            ],
        )
        rslt = rslt.sort_values(
            by=["Counter", "Commitment"], ascending=[False, False]
        )
        # df_stats_to_format = ordered_rpt_items.merge(commitment_df, how='outer', left_on='Name', right_on='Name').\
        #     merge(holding_period_df, how='outer', left_on='Name', right_on='Name').\
        #     merge(max_nav_date, how='outer', left_on='Name', right_on='Name').\
        #     merge(nav_df, how='outer', left_on='Name', right_on='Name').\
        #     merge(irr_melted, how='outer', left_on='Name', right_on='Name').\
        #     merge(multiple_melted, how='outer', left_on='Name', right_on='Name'). \
        #     merge(ks_pme_melted, how='outer', left_on='Name', right_on='Name'). \
        #     merge(direct_alpha_melted, how='outer', left_on='Name', right_on='Name'). \
        #     merge(ror_ctr_melted, how='outer', left_on='Name', right_on='Name')

        ##### ignore all of the below commented out; replaced with more generic recursion
        # top_level = owner
        # group_order = ['PredominantInvestmentType', 'ClassSector', 'Realization']
        # bottom_level = 'Name'
        #
        # rslt = df_stats_to_format[df_stats_to_format.Name == top_level]
        # first_group = group_order[0]
        # first_group_filters = list(attrib[~attrib[first_group].isnull()][first_group].unique())
        #
        # group_filter = first_group_filters[0]
        # for group_filter in first_group_filters:
        #     group_df = df_stats_to_format[df_stats_to_format.Name == group_filter]
        #     rslt = pd.concat([rslt, group_df])
        #
        #     complete_groups = []
        #     complete_filters = []
        #     next_group = [x for x in group_order if x not in complete_groups + [first_group]][0]
        #     for next_group in [x for x in group_order if x not in complete_groups + [first_group]]:
        #         # complete_groups = []
        #
        #         next_group_filters = list(
        #             attrib[attrib[first_group] == group_filter].sort_values(next_group)[next_group].drop_duplicates())
        #         next_group_filter = [x for x in next_group_filters if x not in complete_filters][0]
        #
        #         group_df = df_stats_to_format[df_stats_to_format.Name.isin([next_group_filter])]
        #         rslt = pd.concat([rslt, group_df])
        #         complete_filters.extend([next_group_filter])
        #
        #         if next_group != group_order[-1]:
        #             complete_groups.extend([next_group])
        #             continue
        #         else:
        #             # complete_groups = complete_groups
        #             bottom_level_items = attrib[attrib[next_group] == next_group_filter][
        #                 bottom_level].drop_duplicates().to_list()
        #             bottom_level_df = df_stats_to_format[
        #                 (df_stats_to_format[bottom_level].isin(bottom_level_items))]
        #             rslt = pd.concat([rslt, bottom_level_df])
        #
        #             # attrib[attrib[first_group] == group_filter].sort_values(next_group)[next_group]
        #            # [x for x in next_group_filters if x not in complete_filters]
        #             # complete_groups.extend([next_group])
        #         #
        #         #
        #         #     attrib_group = attrib[
        #         #         (attrib[first_group] == group_filter) & (attrib[next_group] == next_group_filter)]
        #         #     next_groups = [x for x in group_order if x not in complete_groups]
        #         #     if len(next_groups) == 0:
        #         #         bottom_level_items = attrib_group[
        #         #             bottom_level].drop_duplicates().to_list()
        #         #         bottom_level_df = df_stats_to_format[
        #         #             (df_stats_to_format[bottom_level].isin(bottom_level_items))]
        #         #         rslt = pd.concat([rslt, bottom_level_df])
        #         #
        #         # if next_group != group_order[-1]:
        #         #      for next_group_filter in next_group_filters:
        #         #         group_df = df_stats_to_format[df_stats_to_format.Name.isin([next_group_filter])]
        #         #         rslt = pd.concat([rslt, group_df])
        #         #     else:
        #         #         bottom_level_items = attrib_group[
        #         #             bottom_level].drop_duplicates().to_list()
        #         #         bottom_level_df = df_stats_to_format[
        #         #             (df_stats_to_format[bottom_level].isin(bottom_level_items))]
        #         #         rslt = pd.concat([rslt, bottom_level_df])
        #         #         complete_groups.extend([next_group])
        #         #         attrib_group = attrib[(attrib[first_group] == group_filter) & (attrib[next_group] == next_group_filter)]
        #         #         next_groups = [x for x in group_order if x not in complete_groups]
        #         #         if len(next_groups) == 0:
        #         #             bottom_level_items = attrib_group[
        #         #                 bottom_level].drop_duplicates().to_list()
        #         #             bottom_level_df = df_stats_to_format[
        #         #                 (df_stats_to_format[bottom_level].isin(bottom_level_items))]
        #         #             rslt = pd.concat([rslt, bottom_level_df])
        #         #         else:
        #         #             continue
        #
        # #         if group_num + 1 in group_range:
        # #             next_group = group_order[group_num + 1]
        # #
        # #             for group_filter in list(attrib[attrib[group] == group_filter].sort_values(next_group)[next_group].drop_duplicates()):
        # #                 group_df = df_stats_to_format[df_stats_to_format.Name.isin([group_filter])]
        # #                 rslt = pd.concat([rslt, group_df])
        # #
        # #
        # #         else:
        # #             continue
        # #         for z in attrib[attrib[group1] == i].sort_values(group2)[group2].drop_duplicates().to_list():
        # #             group2_df = df_stats_to_format[df_stats_to_format.Name == z]
        # #             rslt = pd.concat([rslt, group2_df])
        # #
        # #             bottom_level_items = attrib[
        # #                 (attrib[group2] == z) & (attrib[bottom_level] != i)][bottom_level].drop_duplicates().to_list()
        # #             bottom_level_df = df_stats_to_format[
        # #                 (df_stats_to_format[bottom_level].isin(bottom_level_items))]
        # #             rslt = pd.concat([rslt, bottom_level_df])
        # #
        # # # existing new
        # # rslt = df_stats_to_format[df_stats_to_format.Name == top_level]
        # # for i in list(attrib[~attrib[group1].isnull()][group1].unique()):
        # #     group1_df = df_stats_to_format[df_stats_to_format.Name == i]
        # #     rslt = pd.concat([rslt, group1_df])
        # #
        # #     for z in attrib[attrib[group1] == i].sort_values(group2)[group2].drop_duplicates().to_list():
        # #         group2_df = df_stats_to_format[df_stats_to_format.Name == z]
        # #         rslt = pd.concat([rslt, group2_df])
        # #
        # #         bottom_level_items = attrib[
        # #             (attrib[group2] == z) & (attrib[bottom_level] != i)][bottom_level].drop_duplicates().to_list()
        # #         bottom_level_df = df_stats_to_format[
        # #             (df_stats_to_format[bottom_level].isin(bottom_level_items))]
        # #         rslt = pd.concat([rslt, bottom_level_df])
        #
        # # old
        # # rslt = df_stats_to_format[df_stats_to_format.Name == owner]
        # # for i in ['Primary Fund', 'Secondary', 'Co-investment/Direct']:
        # #     strat = df_stats_to_format[df_stats_to_format.Name == i]
        # #     rslt = pd.concat([rslt, strat])
        # #     for z in attrib[attrib.PredominantInvestmentType == i].sort_values('ClassSector').ClassSector.drop_duplicates().to_list():
        # #         class_sector = df_stats_to_format[df_stats_to_format.Name == z]
        # #         rslt = pd.concat([rslt, class_sector])
        # #
        # #         investments = attrib[attrib.ClassSector == z].Name.drop_duplicates().to_list()
        # #         investment_stats = df_stats_to_format[df_stats_to_format.Name.isin(investments)].sort_values('Commitment', ascending=False)
        # #         rslt = pd.concat([rslt, investment_stats])
        # attrib_tmp = pd.DataFrame({
        #     'Portfolio': ['Coned'] * 4,
        #     'Region': ['USA', 'EUR'] *2 ,
        #     'Sector': ['TMT', 'Energy', 'Energy', 'Industrials'],
        #     'Deal': ['A', 'B', 'C', 'D'],
        #     'Nav': [20, 13, 50, 12] * 4
        # })
        #
        # report_rslt = pd.DataFrame({
        #     'Name': ['Coned', 'USA', 'EUR', 'TMT', 'Energy', 'Industrials', 'A', 'B', 'C', 'D',
        #              'USA_TMT', 'EUR_Energy', 'USA_Energy', 'EUR_Industials'],
        #     'Nav': [100, 50, 50, 100/4, (100/4)*2, 100/4, 25, 25, 25, 25,
        #             25, 25, 25, 25]
        # })

        # rslt = df_stats_to_format.reset_index(drop=True).drop(columns='NoObs')
        # attrib.PredominantSector = attrib.PredominantSector.str.replace('FUNDS-', '')
        # attrib.PredominantSector = attrib.PredominantSector.str.replace('COS-', '')
        # rslt = rslt.merge(attrib[['PredominantSector', 'ClassSector']].drop_duplicates(),
        #                  how='left', left_on='Name', right_on='ClassSector')
        # rslt.Name = np.where(~rslt.PredominantSector.isnull(), rslt.PredominantSector, rslt.Name)
        # rslt.drop(columns=['PredominantSector', 'ClassSector'], inplace=True)

        # TODO how to reorder multiindex columns?
        columns = [
            "DisplayName",
            "MaxNavDate",
            "Duration",
            "Commitment",
            "Nav",
            ("value", "Incep", "KsPme"),
            ("value", "Incep", "DirectAlpha"),
            ("value", "Incep", "GrossMultiple"),
            ("value", "Incep", "GrossIrr"),
            ("value", "Incep", "AnnRor"),
            ("value", "Incep", "Ctr"),
            ("value", "5Y", "KsPme"),
            ("value", "5Y", "DirectAlpha"),
            ("value", "5Y", "GrossMultiple"),
            ("value", "5Y", "GrossIrr"),
            ("value", "5Y", "AnnRor"),
            ("value", "5Y", "Ctr"),
            ("value", "3Y", "KsPme"),
            ("value", "3Y", "DirectAlpha"),
            ("value", "3Y", "GrossMultiple"),
            ("value", "3Y", "GrossIrr"),
            ("value", "3Y", "AnnRor"),
            ("value", "3Y", "Ctr"),
            ("value", "TTM", "AnnRor"),
            ("value", "TTM", "Ctr"),
            ("value", "QTD", "AnnRor"),
            ("value", "QTD", "Ctr"),
        ]

        for col in columns:
            if col not in list(rslt.columns):
                rslt[col] = None
        rslt = rslt[columns]

        input_data = {
            "Data": rslt,
            "FormatType": ordered_rpt_items[ordered_rpt_items.Layer == 1][
                ["DisplayName"]
            ].drop_duplicates(),
            "FormatSector": ordered_rpt_items[
                ordered_rpt_items.Layer == 2
            ][["DisplayName"]].drop_duplicates(),
            "GroupThree": ordered_rpt_items[ordered_rpt_items.Layer == 3][
                ["DisplayName"]
            ].drop_duplicates(),
        }

        return input_data

    def generate_portfolio_report(self, convert_to_usd=False):
        input_data = self.get_twror_by_industry_rpt(
            owner=self._portfolio_ticker,
            trailing_periods=list(self._trailing_periods),
            convert_to_usd=convert_to_usd,
            list_to_iterate=self._list_to_iterate,
        )
        if input_data is None:
            return

        benchmark_df = self.get_burgiss_bmark()

        as_of_date = dt.datetime.combine(
            self._report_date + MonthEnd(0), dt.datetime.min.time()
        )
        report_json = {
            "Date": pd.DataFrame({"Date": [as_of_date]}),
            "PortfolioName": pd.DataFrame(
                {"PortfolioName": [self._portfolio]},
            ),
            "benchmark_df": benchmark_df,
        }
        report_json.update(input_data)

        # hide cols
        data = input_data.get("Data").copy()
        if (
            data[data.DisplayName == self._portfolio_ticker][
                ("value", "3Y", "AnnRor")
            ].squeeze()
            is None
        ):
            hide_cols = [
                "M",
                "N",
                "O",
                "P",
                "Q",
                "R",
                "S",
                "T",
                "U",
                "V",
                "W",
                "X",
            ]
        elif (
            data[data.DisplayName == self._portfolio_ticker][
                ("value", "5Y", "AnnRor")
            ].squeeze()
            is None
        ):
            hide_cols = ["M", "N", "O", "P", "Q", "R"]
        else:
            hide_cols = None

        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=report_json,
                print_areas={
                    "Industry Breakdown": "B1:AB"
                    + str(len(report_json.get("Data")) + 20)
                },
                hide_cols=hide_cols,
                template="TWROR_Template_threey.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name=f"{self._portfolio_ticker} - {self._portfolio}",
                entity_display_name=f"{self._portfolio_ticker} - {self._portfolio}",
                # entity_ids=[self._portfolio_id],
                # entity_source=DaoSource.PubDwh,
                report_name="PE Performance x Industry and Deal",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.PE,
                report_frequency="Quarterly",
                aggregate_intervals=AggregateInterval.Multi,
                # output_dir="cleansed/investmentsreporting/printedexcels/",
                # report_output_source=DaoSource.DataLake,
            )

    def set_manager_rpt_settings(self, manager_name):
        self._mgr_df = self.get_manager_investments()
        self._portfolio = manager_name
        self._portfolio_ticker = manager_name
        os_list = str(
            list(self._portfolios.OperationalSeriesTicker.unique())
        )
        os_list = os_list.replace("[", "(").replace("]", ")")
        self._os_series = os_list

        self._asset_class_filter = None
        self._type_filter = None
        self._attributes_needed = [
            "Name",
            "PredominantInvestmentType",
            "PredominantSector",
            "PredominantRealizationTypeCategory",
        ]
        self._list_to_iterate = [
            ["Portfolio"],
            ["PredominantInvestmentType"],
            ["PredominantInvestmentType", "PredominantSector"],
            [
                "PredominantInvestmentType",
                "PredominantSector",
                "PredominantRealizationTypeCategory",
            ],
        ]

    def run(self, **kwargs):
        self._portfolios = self.get_portfolio_os()
        # portfolios = self.get_portfolios_pe_only()

        # self._mgr_df = self.get_manager_investments()
        self._mgr_df = None
        error_df = pd.DataFrame()
        # acronyms = ['GIP', 'MACRO', 'YAKUMO']
        portfolio_list = list(
            self._portfolios.PortfolioReportingName.drop_duplicates()
        )
        primaries_os = self.get_portfolio_os_investment_type(
            "Primary Fund"
        )
        portfolio_list = [
            "Trive Capital Management, LLC",
            "Wynnchurch Partners, Ltd.",
            "Peak Rock Capital LLC",
        ]
        # portfolio_list = ['Wynnchurch Partners, Ltd.',
        #                   'Peak Rock Capital LLC']
        portfolios = self.get_pe_only_portfolios()
        # portfolio_list = ['Customized Infrastructure Strategies, L.P.',
        #                   'GCM Grosvenor Customized Infrastructure Strategies III, L.P.']
        # portfolio_list = ['Alpha Z PE Co-Investments I (Master), L.P.', 'GCM Grosvenor Pacific, L.P.', 'Custom Co-Investment Fund 2022, L.P.']
        # portfolio_list = list(portfolios.PortfolioReportingName.unique())
        for x in range(0, len(portfolio_list)):
            try:
                portfolio = portfolio_list[x]
                print(
                    f"{portfolio} - {x} out of {len(portfolio_list)}; {round(x/len(portfolio_list),2)*100}%"
                )
                # portfolio = 'The Consolidated Edison Pension Plan Master Trust - GCM PE Account'
                # portfolio = 'GCM Grosvenor Secondary Opportunities Fund III, L.P.'
                # portfolio = 'GCM Grosvenor Diversified Partners, L.P.'
                # portfolio = 'GCM Grosvenor Private Equity Partners 2020, L.P.'
                # portfolio = 'GCM Grosvenor NYC Emerging RE Managers, L.P.'
                # portfolio = 'Pengana Private Equity Trust'
                # portfolio = 'GCM PE Primaries'
                # portfolio = 'Customized Infrastructure Strategies, L.P.'
                # portfolio = 'GCM Grosvenor Customized Infrastructure Strategies II, L.P.'
                # portfolio = 'GCM Grosvenor Customized Infrastructure Strategies III, L.P.'
                # portfolio = 'GCM Grosvenor Co-Investment Opportunities Fund, L.P.'
                # portfolio = 'GCM Grosvenor Co-Investment Opportunities Fund II, L.P.'
                # portfolio = 'GCM Grosvenor Co-Investment Opportunities Fund III, L.P.'
                # portfolio = 'GCM Co-Investments'
                # self._portfolio = portfolio
                # self._portfolio_ticker = portfolios[portfolios.PortfolioReportingName == portfolio].PortfolioTicker.unique()[0]
                # uniq_port = portfolios[['PortfolioMasterId' , 'PortfolioReportingName']].drop_duplicates()
                # self._portfolio_id = [5]
                # self._portfolio_id = uniq_port[uniq_port.PortfolioReportingName == portfolio].PortfolioMasterId.item()
                os_list = str(
                    list(
                        portfolios[
                            portfolios.PortfolioReportingName == portfolio
                        ].OperationalSeriesTicker
                    )
                )
                os_list = str(
                    list(portfolios.OperationalSeriesTicker.unique())
                )
                os_list = str(list(primaries_os.OperationalSeriesTicker))
                # self._portfolio_ticker = portfolio
                # self._portfolio_ticker = portfolio
                # os_list = str(list(self._mgr_df[self._mgr_df.InvestmentManagerName == portfolio].OsTicker))

                # os_list = os_list.replace('[', '(').replace(']', ')')
                # self._os_series = os_list

                # self._start_date = dt.date(2010, 1, 1)
                # self._type_filter = ['Primary Fund']
                # self._asset_class_filter = ['Private Equity']
                # self._asset_class_filter = None
                # self._type_filter = None

                # self._attributes_needed = ['Name', 'PredominantInvestmentType', 'PredominantSector', 'PredominantRealizationTypeCategory']
                # self._attributes_needed = ['Name', 'PredominantInvestmentType', 'PredominantSector']

                self._list_to_iterate = [
                    ["Portfolio"],
                    ["PredominantInvestmentType"],
                    ["PredominantInvestmentType", "PredominantSector"],
                    [
                        "PredominantInvestmentType",
                        "PredominantSector",
                        "PredominantRealizationTypeCategory",
                    ],
                    ["Name"],
                ]
                # self._list_to_iterate = [['Portfolio'],
                #                          ['PredominantInvestmentType'],
                #                          ['PredominantInvestmentType', 'PredominantSector']
                #                          ]
                self.set_manager_rpt_settings(manager_name=portfolio)
                self._list_to_iterate = [
                    ["Portfolio"],
                    ["PredominantInvestmentType"],
                    ["PredominantInvestmentType", "PredominantSector"],
                    [
                        "PredominantInvestmentType",
                        "PredominantSector",
                        "PredominantRealizationTypeCategory",
                    ],
                    ["Name"],
                ]

                self.generate_portfolio_report(convert_to_usd=True)

            except Exception as e:
                error_msg = getattr(e, "message", repr(e))
                print(error_msg)
                error_df = pd.concat(
                    [
                        pd.DataFrame(
                            {
                                "Portfolio": [self._portfolio],
                                "Date": [self._report_date],
                                "ErrorMessage": [error_msg],
                            }
                        ),
                        error_df,
                    ]
                )

        return error_df


if __name__ == "__main__":
    # report_date = dt.date(2022, 12, 31)
    report_date = dt.date(2022, 9, 30)
    with Scenario(
        dao_config={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                # DaoSource.InvestmentsDwh.name: {
                #     "Environment": "prd",
                #     "Subscription": "prd",
                # },
                DaoSource.PvmDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.DataLake_Blob.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
        as_of_date=report_date,
    ).context():

        RunTwrorIndustry().execute()
