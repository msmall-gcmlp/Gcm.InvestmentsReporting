import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.Utils.tabular_data_util_outputs import TabularDataOutputTypes
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
from gcm.inv.dataprovider.entity_master import EntityMaster
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import (
    AggregateFromDaily,
)
import datetime as dt
import numpy as np
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
import statsmodels.api as sm
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from gcm.inv.scenario import Scenario


class BbaReport(object):
    def __init__(self):
        self._firm_only = Scenario.get_attribute("firm_only")
        self._runner = Scenario.get_attribute("dao")
        self._report_date = Scenario.get_attribute("as_of_date")
        self._strategy_type = "Strategy"
        self._sub_strategy_order = [
            "Emerging Market Credit",
            "Fundamental Credit",
            "Long/Short Credit",
            "Structured Credit",
            "Fundamental Long-Biased",
            "Fundamental Market Neutral",
            "Emerging Market Macro",
            "Global Macro",
            "Multi-Strategy",
            "CTA/Managed Futures",
            "Quantitative",
            "Convertible Arbitrage",
            "Fixed Income Arbitrage",
        ]
        self._portfolio = Portfolio()
        self._portfolio_dimn = self._portfolio.get_dimensions()
        self._strategy_benchmark = StrategyBenchmark()
        self._investment_group = InvestmentGroup()
        self._entity_master = EntityMaster()
        self._factor_returns = AggregateFromDaily().transform(
            data=Factor(
                tickers=["SPXT Index", "SBMMTB1 Index"]
            ).get_returns(
                start_date=dt.date(2010, 1, 1),
                end_date=self._report_date + MonthEnd(1),
                fill_na=True,
            ),
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True,
        )
        self._gcm, self._eh = self.get_allocation_data_portfolio()
        (
            self._gcm_firmwide,
            self._gcm_firmwide_multistrat,
        ) = self.get_allocation_data_firmwide()
        self._trailing_periods = [
            1,
            self._report_date.month
            - dt.date(
                self._report_date.year,
                3 * ((self._report_date.month - 1) // 3) + 1,
                1,
            ).month
            + 1,
            self._report_date.month,
            12,
            36,
            60,
            120,
        ]
        self._trailing_period_df = pd.DataFrame(
            {
                "Period": ["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y"],
                "TrailingPeriod": [
                    1,
                    self._report_date.month
                    - dt.date(
                        self._report_date.year,
                        3 * ((self._report_date.month - 1) // 3) + 1,
                        1,
                    ).month
                    + 1,
                    self._report_date.month,
                    12,
                    36,
                    60,
                    120,
                ],
            }
        )
        self._period_order = [
            "MTD",
            "QTD",
            "YTD",
            "TTM",
            "3Y",
            "5Y",
            "10Y",
        ]
        self._acronym_input = Scenario.get_attribute("acronyms")

    @property
    def _strategy_order(self):
        if self._strategy_type == "Strategy":
            return [
                "Credit",
                "Long/Short Equity",
                "Macro",
                "Multi-Strategy",
                "Quantitative",
                "Relative Value",
            ]
        else:
            return [
                "Emerging Market Credit",
                "Fundamental Credit",
                "Long/Short Credit",
                "Structured Credit",
                "Fundamental Long-Biased",
                "Fundamental Market Neutral",
                "Emerging Market Macro",
                "Global Macro",
                "Multi-Strategy",
                "CTA/Managed Futures",
                "Quantitative",
                "Convertible Arbitrage",
                "Fixed Income Arbitrage",
            ]

    @property
    def _all_acronyms(self):
        return self._gcm[
            self._gcm.Date == self._report_date
        ].Acronym.unique()

    @property
    def _selected_acronyms(self):
        if self._acronym_input is None:
            return self._all_acronyms
        else:
            return self._acronym_input

    def get_excess_return_rpt(
        self, port_rtn, bmark_rtn, dollar_size=False
    ):
        ytd_excess = self.get_excess_return_stats(
            dt.date(self._report_date.year, 1, 1),
            port_rtn,
            bmark_rtn,
            dollar_size,
        ).rename(columns={"value": "YTD"})
        ttm_excess = self.get_excess_return_stats(
            self._report_date
            - relativedelta(years=1)
            + relativedelta(months=1),
            port_rtn,
            bmark_rtn,
            dollar_size,
        ).rename(columns={"value": "TTM"})
        threey_excess = self.get_excess_return_stats(
            self._report_date
            - relativedelta(years=3)
            + relativedelta(months=1),
            port_rtn,
            bmark_rtn,
            dollar_size,
        ).rename(columns={"value": "3Y"})
        # dumb way to annualize but get it out the door
        threey_excess.loc[
            [
                "CtrTotal",
                "CtrContrib_Outperformer",
                "CtrContrib_Underperformer",
                "AvgExcess_Outperformer",
                "AvgExcess_Underperformer",
            ]
        ] = (
            1
            + threey_excess.loc[
                [
                    "CtrTotal",
                    "CtrContrib_Outperformer",
                    "CtrContrib_Underperformer",
                    "AvgExcess_Outperformer",
                    "AvgExcess_Underperformer",
                ]
            ]
        ) ** (
            1 / 3
        ) - 1

        fivey_excess = self.get_excess_return_stats(
            self._report_date
            - relativedelta(years=5)
            + relativedelta(months=1),
            port_rtn,
            bmark_rtn,
            dollar_size,
        ).rename(columns={"value": "5Y"})

        # dumb way to annualize but get it out the door
        fivey_excess.loc[
            [
                "CtrTotal",
                "CtrContrib_Outperformer",
                "CtrContrib_Underperformer",
                "AvgExcess_Outperformer",
                "AvgExcess_Underperformer",
            ]
        ] = (
            1
            + fivey_excess.loc[
                [
                    "CtrTotal",
                    "CtrContrib_Outperformer",
                    "CtrContrib_Underperformer",
                    "AvgExcess_Outperformer",
                    "AvgExcess_Underperformer",
                ]
            ]
        ) ** (
            1 / 5
        ) - 1

        result = (
            ytd_excess.merge(
                ttm_excess, how="left", left_index=True, right_index=True
            )
            .merge(
                threey_excess,
                how="left",
                left_index=True,
                right_index=True,
            )
            .merge(
                fivey_excess, how="left", left_index=True, right_index=True
            )[["YTD", "TTM", "3Y", "5Y"]]
        )
        return result

    def get_excess_return_stats(
        self, start_date, port_rtn, bmark_rtn, dollar_size=False
    ):
        port_rtn = port_rtn[
            (port_rtn.Date >= start_date)
            & (port_rtn.Date <= self._report_date)
        ]
        if pd.to_datetime(start_date) != port_rtn.Date.min():
            return pd.DataFrame(
                {
                    "Name": [
                        "CtrTotal",
                        "CtrContrib_Outperformer",
                        "CtrContrib_Underperformer",
                        "HitRate_Outperformer",
                        "HitRate_Underperformer",
                        "Blank1",
                        "AvgExcessRatio",
                        "AvgExcess_Outperformer",
                        "AvgExcess_Underperformer",
                        "Blank2",
                        "AvgSizeRatio",
                        "AvgSize_Outperformer",
                        "AvgSize_Underperformer",
                    ],
                    "value": [None] * 13,
                }
            ).set_index("Name")
        port_rtn.Date = pd.to_datetime(port_rtn.Date)
        bmark_rtn.Date = pd.to_datetime(bmark_rtn.Date)
        return_df = (
            port_rtn[
                [
                    "Date",
                    "InvestmentGroupName",
                    "InvestmentGroupId",
                    "OpeningBalance",
                    "pct_investment_of_portfolio_total",
                    "Ror",
                ]
            ]
            .merge(
                bmark_rtn,
                how="left",
                left_on=["Date", "InvestmentGroupId"],
                right_on=["Date", "InvestmentGroupId"],
            )
            .fillna(0)
        )
        assert len(port_rtn) == len(return_df)

        return_df["Excess"] = return_df.Ror - return_df.BmarkRor

        funds_classified = (
            return_df[["InvestmentGroupName", "Excess"]]
            .groupby(["InvestmentGroupName"])
            .sum()
            .reset_index()
        )
        funds_classified["Type"] = np.where(
            funds_classified.Excess > 0, "Outperformer", "Underperformer"
        )

        port_classified = return_df.merge(
            funds_classified[["InvestmentGroupName", "Type"]],
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        )
        assert len(port_classified) == len(port_rtn)
        assert len(port_classified) == len(return_df)

        port_classified["Ctr"] = (
            port_classified.Excess
            * port_classified.pct_investment_of_portfolio_total
        )
        ctr = self.calc_ctr(
            port_classified.pivot_table(
                index="Date", columns="InvestmentGroupName", values="Ctr"
            ).fillna(0)
        ).reset_index()
        ctr_classified = ctr.merge(
            funds_classified,
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        )

        ctr_contrib = (
            ctr_classified[["Type", "CTR"]]
            .groupby("Type")
            .sum()
            .reindex(["Outperformer", "Underperformer"])
        )
        ctr_total = ctr_classified.CTR.sum()

        hit_rate = (
            ctr_classified[["Type", "InvestmentGroupName"]]
            .groupby("Type")
            .count()
            .reindex(["Outperformer", "Underperformer"])
        )
        hit_rate["pct"] = hit_rate / hit_rate.sum()

        excess_return_series = return_df.pivot_table(
            index="Date", columns="InvestmentGroupName", values="Excess"
        )
        inv_stats = pd.DataFrame()
        for i in excess_return_series.columns:
            # if return_df[[i]][return_df.index==self._report_date].isnull().squeeze():
            #     continue
            inv_stats = pd.concat(
                [
                    inv_stats,
                    self.ann_return(
                        excess_return_series[[i]],
                        trailing_periods=["Incep"],
                        freq=12,
                        return_NoObs=True,
                    ),
                ]
            )

        inv_stats_classified = inv_stats.merge(
            funds_classified,
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        )
        avg_excess = (
            inv_stats_classified[["Type", "AnnRor"]]
            .groupby("Type")
            .mean()
            .reindex(["Outperformer", "Underperformer"])
        )
        avg_excess_ratio = (
            avg_excess[avg_excess.index == "Outperformer"]
            .AnnRor.abs()
            .squeeze()
            / avg_excess[avg_excess.index == "Underperformer"]
            .AnnRor.abs()
            .squeeze()
        )
        if dollar_size:
            avg_size = (
                port_classified[["Type", "OpeningBalance"]]
                .groupby("Type")
                .mean()
                .reindex(["Outperformer", "Underperformer"])
                / 1e6
            )
            avg_size.rename(
                columns={"OpeningBalance": "AvgSize"}, inplace=True
            )
        else:
            avg_size = (
                port_classified[
                    ["Type", "pct_investment_of_portfolio_total"]
                ]
                .groupby("Type")
                .mean()
                .reindex(["Outperformer", "Underperformer"])
            )
            avg_size.rename(
                columns={"pct_investment_of_portfolio_total": "AvgSize"},
                inplace=True,
            )

        avg_size_ratio = (
            avg_size[avg_size.index == "Outperformer"]
            .AvgSize.abs()
            .squeeze()
            / avg_size[avg_size.index == "Underperformer"]
            .AvgSize.abs()
            .squeeze()
        )

        totals = pd.DataFrame(
            {
                "Name": ["CtrTotal", "AvgExcessRatio", "AvgSizeRatio"],
                "value": [ctr_total, avg_excess_ratio, avg_size_ratio],
            }
        )
        breakdowns = (
            ctr_contrib.merge(
                avg_excess, how="left", left_index=True, right_index=True
            )
            .merge(avg_size, how="left", left_index=True, right_index=True)
            .merge(
                hit_rate[["pct"]],
                how="left",
                left_index=True,
                right_index=True,
            )
            .rename(
                columns={
                    "CTR": "CtrContrib",
                    "AnnRor": "AvgExcess",
                    "OpeningBalance": "AvgSize",
                    "pct": "HitRate",
                }
            )
        )
        breakdowns = pd.melt(breakdowns.reset_index(), id_vars=["Type"])
        breakdowns["Name"] = (
            breakdowns.variable.astype("str")
            + "_"
            + breakdowns.Type.astype("str")
        )

        result = (
            pd.concat([totals, breakdowns[["Name", "value"]]])
            .set_index("Name")
            .reindex(
                [
                    "CtrTotal",
                    "CtrContrib_Outperformer",
                    "CtrContrib_Underperformer",
                    "HitRate_Outperformer",
                    "HitRate_Underperformer",
                    "Blank1",
                    "AvgExcessRatio",
                    "AvgExcess_Outperformer",
                    "AvgExcess_Underperformer",
                    "Blank2",
                    "AvgSizeRatio",
                    "AvgSize_Outperformer",
                    "AvgSize_Underperformer",
                ]
            )
        )

        return result
        # uncomment this if we want to determine over/under performers by ann ROR - instead using sum of monthlies
        # bmark_stats = self.ann_return(
        #     bmark_rtn, trailing_periods=self._trailing_periods, freq=12, return_NoObs=True)
        # trailing_periods = self._trailing_period_df[self._trailing_period_df.Period.isin(['YTD', 'TTM', '3Y', '5Y'])].TrailingPeriod.to_list()
        # return_df = port_rtn.pivot_table(index='Date', columns='InvestmentGroupName', values='Ror').loc[:self._report_date]
        # port_stats = pd.DataFrame()
        # for i in return_df.columns:
        #     if return_df[[i]][return_df.index==self._report_date].isnull().squeeze():
        #         continue
        #     port_stats = pd.concat([port_stats, self.ann_return(
        #         return_df[[i]], trailing_periods=trailing_periods, freq=12, return_NoObs=True)])
        #
        # stats_merged = port_stats[['InvestmentGroupName', 'AnnRor', 'NoObs']].rename(columns={'AnnRor': 'GcmRor'}).
        # merge(bmark_stats.rename(columns={'AnnRor': 'BmarkRor'}), how='left', left_on='NoObs', right_on='NoObs').merge(
        #     self._trailing_period_df, how='left', left_on='NoObs',
        #     right_on='TrailingPeriod').drop_duplicates()[['InvestmentGroupName', 'GcmRor', 'BmarkRor', 'Period']]
        # stats_merged['Type'] = np.where(stats_merged.GcmRor > stats_merged.BmarkRor, 'Outperformer', 'Underperformer')
        #
        # port_rtn.stats_merged.merge()

    def get_sharpe_rpt(self, port_rtn, bmark_rtn):
        bmark_stats = self.calc_sharpe(
            bmark_rtn, trailing_periods=self._trailing_periods
        )

        port_stats = self.calc_sharpe(
            port_rtn, trailing_periods=self._trailing_periods
        )

        sharpe_df = (
            bmark_stats.merge(
                port_stats, how="left", left_on="NoObs", right_on="NoObs"
            )
            .merge(
                self._trailing_period_df,
                how="left",
                left_on="NoObs",
                right_on="TrailingPeriod",
            )
            .drop_duplicates()
            .rename(columns={"Sharpe_x": "EHI200", "Sharpe_y": "GCM"})
        )

        return (
            sharpe_df[["Period", "GCM", "EHI200"]]
            .set_index("Period")
            .reindex(self._period_order)
        )

    def calc_sharpe(self, returns, trailing_periods):
        rf = self._factor_returns[["SBMMTB1 Index"]].rename(
            columns={"SBMMTB1 Index": "rf"}
        )
        returns = returns.dropna()

        result = pd.DataFrame()
        for trailing_period in trailing_periods:
            if trailing_period < 12:
                continue
            if trailing_period != "Incep":
                if len(returns) < trailing_period:
                    continue
                else:
                    return_sub = returns.tail(trailing_period)
            else:
                return_sub = returns
            rf_ror = self.ann_return(rf, [trailing_period], freq=12)
            asset_ror = self.ann_return(
                return_sub, [trailing_period], freq=12
            )
            asset_vol = return_sub.std() * np.sqrt(12)

            sharpe_df = pd.DataFrame(
                {
                    "Sharpe": [
                        (
                            asset_ror.AnnRor.squeeze()
                            - rf_ror.AnnRor.squeeze()
                        )
                        / asset_vol.squeeze()
                    ],
                    "NoObs": trailing_period,
                }
            )
            result = result.append(sharpe_df)
        return result

    def ann_vol(self, returns, trailing_periods, freq=250):
        result = pd.DataFrame()
        for trailing_period in trailing_periods:
            if trailing_period != "Incep":
                if len(returns) < trailing_period:
                    continue
                if trailing_period < 12:
                    continue
                else:
                    return_sub = returns.tail(trailing_period)
            else:
                return_sub = returns
            ann_vol = pd.DataFrame(return_sub.std() * np.sqrt(freq))
            ann_vol["NoObs"] = len(return_sub)
            result = result.append(ann_vol)
        result = result.reset_index().rename(
            columns={"index": "Name", 0: "AnnVol"}
        )

        return result

    def get_vol_rpt(self, port_rtn, bmark_rtn):
        bmark_stats = self.ann_vol(
            bmark_rtn, trailing_periods=self._trailing_periods, freq=12
        )

        port_stats = self.ann_vol(
            port_rtn, trailing_periods=self._trailing_periods, freq=12
        )

        vol_df = (
            bmark_stats.merge(
                port_stats, how="left", left_on="NoObs", right_on="NoObs"
            )
            .merge(
                self._trailing_period_df,
                how="left",
                left_on="NoObs",
                right_on="TrailingPeriod",
            )
            .drop_duplicates()
            .rename(columns={"AnnVol_x": "EHI200", "AnnVol_y": "GCM"})
        )

        return (
            vol_df[["Period", "GCM", "EHI200"]]
            .set_index("Period")
            .reindex(self._period_order)
        )

    def get_downside_rpt(self, port_rtn, bmark_rtn, assets, benchmarks):
        spx = self._factor_returns[["SPXT Index"]].rename(
            columns={"SPXT Index": "SPXT"}
        )
        returns = spx.merge(
            bmark_rtn, how="left", left_index=True, right_index=True
        ).merge(port_rtn, how="left", left_index=True, right_index=True)

        downside_capture = self.calculate_downside_capture(
            returns, assets, benchmarks, self._trailing_periods
        )
        downside_capture_df = downside_capture.merge(
            self._trailing_period_df,
            how="left",
            left_on="NoObs",
            right_on="TrailingPeriod",
        ).drop_duplicates()
        result = downside_capture_df[
            ["Asset", "DownsideCapture", "Period"]
        ].pivot(index="Period", columns="Asset", values="DownsideCapture")[
            assets
        ]
        return result.reindex(self._period_order)

    def get_correlation_rpt(self, port_rtn, bmark_rtn, assets, benchmarks):
        spx = self._factor_returns[["SPXT Index"]].rename(
            columns={"SPXT Index": "SPXT"}
        )
        returns = spx.merge(
            bmark_rtn, how="left", left_index=True, right_index=True
        ).merge(port_rtn, how="left", left_index=True, right_index=True)

        correlation = self.calculate_correlation(
            returns, assets, benchmarks, self._trailing_periods
        )
        correlation_df = correlation.merge(
            self._trailing_period_df,
            how="left",
            left_on="NoObs",
            right_on="TrailingPeriod",
        ).drop_duplicates()
        result = correlation_df[["Asset", "Correlation", "Period"]].pivot(
            index="Period", columns="Asset", values="Correlation"
        )[assets]
        return result.reindex(self._period_order)

    def calculate_correlation(
        self, returns, assets, benchmarks, trailing_periods
    ):
        result = pd.DataFrame()
        for asset in assets:
            for bmk in benchmarks:
                rtns = returns[[asset, bmk]].dropna()
                for trailing_period in trailing_periods:
                    if trailing_period < 12:
                        continue
                    if trailing_period != "Incep":
                        if len(rtns) < trailing_period:
                            continue
                        else:
                            return_sub = rtns.tail(trailing_period)
                    else:
                        return_sub = rtns
                    corr_df = pd.DataFrame(
                        {
                            "Asset": [asset],
                            "Correlation": [
                                return_sub.corr()[asset].loc[bmk]
                            ],
                            "NoObs": [trailing_period],
                        }
                    )
                    result = result.append(corr_df)
        return result

    def calculate_downside_capture(
        self, returns, assets, benchmarks, trailing_periods
    ):
        result = pd.DataFrame()
        for asset in assets:
            for bmk in benchmarks:
                rtns = returns[[asset, bmk]].dropna()
                for trailing_period in trailing_periods:
                    if trailing_period != "Incep":
                        if len(rtns) < trailing_period:
                            continue
                        if trailing_period < 12:
                            continue
                        else:
                            return_sub = rtns.tail(trailing_period)
                    else:
                        return_sub = rtns
                    rtns_neg_bmark = return_sub[return_sub[bmk] < 0]
                    downside_df = pd.DataFrame(
                        {
                            "Asset": [asset],
                            "DownsideCapture": [
                                rtns_neg_bmark[asset].mean()
                                / rtns_neg_bmark[bmk].mean()
                            ],
                            "NoObs": [trailing_period],
                        }
                    )
                    result = result.append(downside_df)
        return result

    def get_betas_rpt(self, port_rtn, bmark_rtn, assets, benchmarks):
        spx = self._factor_returns[["SPXT Index"]].rename(
            columns={"SPXT Index": "SPXT"}
        )
        returns = spx.merge(
            bmark_rtn, how="left", left_index=True, right_index=True
        ).merge(port_rtn, how="left", left_index=True, right_index=True)

        betas = self.calculate_betas(
            returns, assets, benchmarks, self._trailing_periods
        )
        betas_df = betas.merge(
            self._trailing_period_df,
            how="left",
            left_on="NoObs",
            right_on="TrailingPeriod",
        ).drop_duplicates()

        result = betas_df[["Name", "Beta", "Period"]].pivot(
            index="Period", columns="Name", values="Beta"
        )[assets]
        return result.reindex(self._period_order)

    def calculate_betas(
        self, returns, assets, benchmarks, trailing_periods
    ):
        returns["intercept"] = 1

        result = pd.DataFrame()
        for asset in assets:
            for bmk in benchmarks:
                rtns = returns[[asset, bmk, "intercept"]].dropna()
                for trailing_period in trailing_periods:
                    if trailing_period != "Incep":
                        if trailing_period < 12:
                            continue
                        if len(rtns) < trailing_period:
                            continue
                        else:
                            return_sub = rtns.tail(trailing_period)
                    else:
                        return_sub = rtns
                    return_trimmed = return_sub.dropna()
                    fit = sm.OLS(
                        return_trimmed[asset],
                        return_trimmed[[bmk, "intercept"]],
                    ).fit()
                    alpha_df = pd.DataFrame(
                        [
                            [
                                asset,
                                bmk,
                                fit.params[bmk],
                                fit.params.intercept * 12,
                                fit.rsquared_adj,
                                len(return_sub),
                            ]
                        ],
                        columns=[
                            "Name",
                            "Benchmark",
                            "Beta",
                            "AnnAlpha",
                            "AdjR2",
                            "NoObs",
                        ],
                    )
                    result = result.append(alpha_df)
        result = result.loc[
            (result.Name != "intercept")
            & (result.Benchmark != "intercept")
        ]
        return result

    def get_returns_rpt(self, port_rtn, bmark_rtn):
        bmark_stats = self.ann_return(
            bmark_rtn,
            trailing_periods=self._trailing_periods,
            freq=12,
            return_NoObs=True,
        )

        port_stats = self.ann_return(
            port_rtn,
            trailing_periods=self._trailing_periods,
            freq=12,
            return_NoObs=True,
        )

        return_df = (
            bmark_stats.merge(
                port_stats, how="left", left_on="NoObs", right_on="NoObs"
            )
            .merge(
                self._trailing_period_df,
                how="left",
                left_on="NoObs",
                right_on="TrailingPeriod",
            )
            .drop_duplicates()
            .rename(columns={"AnnRor_x": "Bmark", "AnnRor_y": "GCM"})
        )
        return_df["excess"] = return_df.GCM - return_df.Bmark
        return_df.excess = np.where(
            return_df.GCM.isnull(), None, return_df.excess
        )

        return (
            return_df[["Period", "GCM", "Bmark", "excess"]]
            .set_index("Period")
            .reindex(self._period_order)
        )

    def get_portfolio_bba_outliers(
        self, acronyms, gcm, bmark, start_date, end_date, trailing_period
    ):
        df = self.get_strategy_sizing_outliers(
            acronyms, gcm, bmark, start_date, end_date, trailing_period
        )
        top_10_strategy_attrib = self.get_top_n_portfolios(
            df=df, col_name="StrategyAttrib", n=10, asc=False
        )
        bottom_10_strategy_attrib = self.get_top_n_portfolios(
            df=df, col_name="StrategyAttrib", n=10, asc=True
        )
        top_10_manager_attrib = self.get_top_n_portfolios(
            df=df, col_name="ManagerAttrib", n=10, asc=False
        )
        bottom_10_manager_attrib = self.get_top_n_portfolios(
            df=df, col_name="ManagerAttrib", n=10, asc=True
        )
        input_data = {
            "top_strategy_attrib": top_10_strategy_attrib[["Acronym"]],
            "top_strategy_attrib_values": top_10_strategy_attrib[
                ["StrategyAttrib"]
            ],
            "bottom_strategy_attrib": bottom_10_strategy_attrib[
                ["Acronym"]
            ],
            "bottom_strategy_attrib_values": bottom_10_strategy_attrib[
                ["StrategyAttrib"]
            ],
            "top_manager_attrib": top_10_manager_attrib[["Acronym"]],
            "top_manager_attrib_values": top_10_manager_attrib[
                ["ManagerAttrib"]
            ],
            "bottom_manager_attrib": bottom_10_manager_attrib[["Acronym"]],
            "bottom_manager_attrib_values": bottom_10_manager_attrib[
                ["ManagerAttrib"]
            ],
        }
        return input_data

    def get_top_n_portfolios(self, df, col_name, n, asc):
        result = df.sort_values(col_name, ascending=asc).head(n)
        # result = top_n.merge(self._portfolio_dimn[['Acronym', 'StrategyMandate']].drop_duplicates(),
        #                       how='left', left_on='Acronym', right_on='Acronym')
        # if asc:
        #     result = result[result[col_name] < 0]
        # else:
        #     result = result[result[col_name] > 0]
        return result[["Acronym", col_name]]

    def get_strategy_sizing_outliers(
        self, acronyms, gcm, bmark, start_date, end_date, trailing_period
    ):
        self._strategy_type = "Strategy"
        error_df = pd.DataFrame()
        attribution_by_portfolio = pd.DataFrame()

        mandate_filter = self._portfolio_dimn[
            ~self._portfolio_dimn.StrategyMandate.isin(
                [
                    "Multi-Strategy",
                    "Opportunistic",
                    "Global Long / Short - Low Beta",
                ]
            )
        ].Acronym.to_list()
        remove_acronyms_not_multistrat_df = (
            gcm[["Date", "Acronym", "pct_strategy_of_portfolio_total"]][
                (gcm.Date >= start_date) & (gcm.Date <= end_date)
            ]
            .drop_duplicates()
            .groupby(["Date", "Acronym"])
            .count()
            .groupby("Acronym")
            .mean()
            .reset_index()
        )

        remove_acronyms_not_multistrat = (
            remove_acronyms_not_multistrat_df[
                remove_acronyms_not_multistrat_df.pct_strategy_of_portfolio_total
                < 3
            ]
            .sort_values("Acronym")
            .Acronym.drop_duplicates()
            .to_list()
        )
        remove_tickers_aum_df = gcm[gcm.Date == end_date][
            ["Acronym", "TotalDollarPortfolio"]
        ].drop_duplicates()
        remove_tickers_aum = remove_tickers_aum_df[
            remove_tickers_aum_df.TotalDollarPortfolio < 100000000
        ].Acronym.to_list()
        hard_code_exclude = [
            "IFCD",
            "PIKEPLACE",
            "ANCHOR4B",
            "RAVEN8",
            "RAVEN7",
            "LUPINEC",
            "RAVEN6",
            "RAVEN4",
            "SINGULARB",
            "SINGULAR B",
        ]
        acronyms_to_run = list(
            filter(
                lambda x: x not in remove_acronyms_not_multistrat
                and x not in remove_tickers_aum
                and x not in hard_code_exclude
                and x not in mandate_filter,
                acronyms,
            )
        )

        for acronym in acronyms_to_run:
            print(acronym)
            try:
                gcm_df = gcm[gcm.Acronym == acronym]
                portfolio_attrib = self.get_attribution_rpt(
                    gcm=gcm_df.copy(),
                    bmark=bmark.copy(),
                    start_date=start_date,
                    end_date=end_date,
                    ctr=None,
                )
                portfolio_attrib["Acronym"] = acronym

                # by strat, will be summed to total portfolio
                attribution_by_portfolio = pd.concat(
                    [attribution_by_portfolio, portfolio_attrib]
                )

            except Exception as e:
                error_msg = getattr(e, "message", repr(e))
                print(error_msg)
                error_df = pd.concat(
                    [
                        pd.DataFrame(
                            {
                                "Portfolio": [acronym],
                                "Date": [end_date],
                                "ErrorMessage": [error_msg],
                            }
                        ),
                        error_df,
                    ]
                )

        attribution_by_portfolio = attribution_by_portfolio.fillna(0)
        attribution_by_portfolio["StrategyAttrib"] = (
            attribution_by_portfolio["StrategySizing"]
            + attribution_by_portfolio["StrategySelection"]
        )
        attribution_by_portfolio["ManagerAttrib"] = (
            attribution_by_portfolio["ManagerSizing"]
            + attribution_by_portfolio["ManagerSelection"]
        )

        result = (
            attribution_by_portfolio[
                ["Acronym", "StrategyAttrib", "ManagerAttrib"]
            ]
            .fillna(0)
            .groupby("Acronym")
            .sum()
            .reset_index()
        )
        return result

    def get_attribution_rpt(self, gcm, bmark, start_date, end_date, ctr):
        if self._strategy_type != "SubStrategy":
            gcm.drop(
                columns=[
                    "pct_strategy_of_portfolio_total",
                    "Strategy",
                    "pct_investment_of_portfolio_strategy_total",
                ],
                inplace=True,
            )
            gcm.rename(
                columns={
                    "pct_substrategy_of_portfolio_total": "pct_strategy_of_portfolio_total",
                    "SubStrategy": "Strategy",
                    "pct_investment_of_portfolio_substrategy_total": "pct_investment_of_portfolio_strategy_total",
                },
                inplace=True,
            )
            bmark.drop(
                columns=["pct_strategy_of_total", "Strategy"], inplace=True
            )
            bmark.rename(
                columns={
                    "pct_substrategy_of_total": "pct_strategy_of_total",
                    "SubStrategy": "Strategy",
                },
                inplace=True,
            )

        gcm_alloc = (
            gcm[["Date", "Strategy", "pct_strategy_of_portfolio_total"]]
            .rename(
                columns={
                    "pct_strategy_of_portfolio_total": "GcmAllocation"
                }
            )
            .drop_duplicates()
        )
        bmark_alloc = (
            bmark[["Date", "Strategy", "pct_strategy_of_total"]]
            .rename(columns={"pct_strategy_of_total": "BmarkAllocation"})
            .drop_duplicates()
        )
        gcm_cap_ror = self.get_returns_from_allocs(
            df=gcm,
            group_cols=["Date", "Strategy"],
            multiply_x="Ror",
            multiply_y="pct_investment_of_portfolio_strategy_total",
        )

        gcm_eq_ror = self.get_returns_from_allocs(
            df=gcm, group_cols=["Date", "Strategy"], equal_weight=True
        )

        bmark_eq_ror = self.get_returns_from_allocs(
            df=bmark, group_cols=["Date", "Strategy"], equal_weight=True
        )
        bmark_ror = self.get_returns_from_allocs(
            df=bmark, group_cols=["Date"], equal_weight=True
        ).rename(columns={"Ror": "BmarkRor"})

        if len(gcm_cap_ror[gcm_cap_ror.Date == start_date]) == 0:
            manager_sizing = pd.DataFrame(
                {"ManagerSelection": [None], "ManagerSizing": [None]}
            ).reindex(self._sub_strategy_order)
            strategy_sizing = pd.DataFrame(
                {"StrategySelection": [None], "StrategySizing": [None]}
            ).reindex(self._sub_strategy_order)
        else:
            strategy_sizing = self.get_strategy_sizing(
                gcm_alloc,
                bmark_alloc,
                bmark_eq_ror,
                bmark_ror,
                start_date,
                end_date,
            )
            manager_sizing = self.get_manager_selection(
                gcm_alloc,
                gcm_cap_ror,
                gcm_eq_ror,
                bmark_eq_ror,
                start_date,
                end_date,
            )

        if self._strategy_type == "SubStrategy":
            result = strategy_sizing.merge(
                manager_sizing, left_index=True, right_index=True
            ).fillna(0)
        else:
            strat_df = self._gcm[
                ["Strategy", "SubStrategy"]
            ].drop_duplicates()
            result = (
                strategy_sizing.merge(
                    manager_sizing, left_index=True, right_index=True
                )
                .merge(
                    strat_df,
                    how="left",
                    left_index=True,
                    right_on="SubStrategy",
                )
                .reset_index(drop=True)
            )
            result = (
                result[
                    [
                        "Strategy",
                        "StrategySelection",
                        "StrategySizing",
                        "ManagerSelection",
                        "ManagerSizing",
                    ]
                ]
                .fillna(0)
                .groupby("Strategy")
                .sum()
            )
        result[result.columns] = np.where(
            result[result.columns] == 0, None, result[result.columns]
        )
        if manager_sizing.sum().sum() != 0 and ctr is not None:
            scale = (
                ctr.tail(1).CTR_x - ctr.tail(1).CTR_y
            ) / result.sum().sum()
            result = result * scale.squeeze()

        return result

    def get_manager_selection(
        self,
        gcm_alloc,
        gcm_cap_ror,
        gcm_eq_ror,
        bmark_eq_ror,
        start_date,
        end_date,
    ):
        df_alloc_ror = (
            gcm_alloc.merge(
                gcm_cap_ror.rename(columns={"Ror": "GcmRor"}),
                how="left",
                left_on=["Date", "Strategy"],
                right_on=["Date", "Strategy"],
            )
            .merge(
                bmark_eq_ror.rename(columns={"Ror": "BmarkRor"}),
                how="left",
                left_on=["Date", "Strategy"],
                right_on=["Date", "Strategy"],
            )
            .fillna(0)
        )
        df_alloc_ror["Ror_less_Bmark"] = (
            df_alloc_ror.GcmRor - df_alloc_ror.BmarkRor
        )
        df_alloc_ror["Ctr"] = (
            df_alloc_ror.GcmAllocation * df_alloc_ror.Ror_less_Bmark
        )

        ctr_df = (
            self.calc_ctr(
                df_alloc_ror.pivot_table(
                    index="Date", columns="Strategy", values="Ctr"
                )
                .fillna(0)
                .loc[start_date:end_date]
            )
            .reindex(self._sub_strategy_order)
            .rename(columns={"CTR": "ManagerSelection"})
        )

        df_gcm_eq_cap = (
            gcm_alloc.merge(
                gcm_cap_ror.rename(columns={"Ror": "GcmCapRor"}),
                how="left",
                left_on=["Date", "Strategy"],
                right_on=["Date", "Strategy"],
            )
            .merge(
                gcm_eq_ror.rename(columns={"Ror": "GcmEqlRor"}),
                how="left",
                left_on=["Date", "Strategy"],
                right_on=["Date", "Strategy"],
            )
            .fillna(0)
        )

        df_gcm_eq_cap["Cap_less_eql"] = (
            df_gcm_eq_cap.GcmCapRor - df_gcm_eq_cap.GcmEqlRor
        )
        df_gcm_eq_cap["Ctr"] = (
            df_gcm_eq_cap.GcmAllocation * df_gcm_eq_cap.Cap_less_eql
        )

        ctr_eql_df = (
            self.calc_ctr(
                df_gcm_eq_cap.pivot_table(
                    index="Date", columns="Strategy", values="Ctr"
                )
                .fillna(0)
                .loc[start_date:end_date]
            )
            .reindex(self._sub_strategy_order)
            .rename(columns={"CTR": "ManagerSizing"})
        )

        ctr_rslt = ctr_df.merge(
            ctr_eql_df, how="left", left_index=True, right_index=True
        )
        ctr_rslt.ManagerSelection = (
            ctr_rslt.ManagerSelection - ctr_rslt.ManagerSizing
        )

        return ctr_rslt

    def get_strategy_sizing(
        self,
        gcm_alloc,
        bmark_alloc,
        bmark_eq_ror,
        bmark_ror,
        start_date,
        end_date,
    ):
        allocs = bmark_alloc.merge(
            gcm_alloc,
            how="left",
            left_on=["Date", "Strategy"],
            right_on=["Date", "Strategy"],
        ).fillna(0)
        allocs["Allocation"] = (
            allocs.GcmAllocation - allocs.BmarkAllocation
        )
        allocs["GcmMissing"] = np.where(
            allocs.GcmAllocation == 0, "Selection", "Sizing"
        )

        returns = bmark_eq_ror.merge(
            bmark_ror, how="left", left_on=["Date"], right_on=["Date"]
        ).fillna(0)
        returns["Ror_less_Bmark"] = returns.Ror - returns.BmarkRor
        # use just strategy ror for bhb rather than bf formula
        # returns['Ror_less_Bmark'] = returns.Ror

        df_alloc_ror = allocs[
            ["Date", "Strategy", "Allocation", "GcmMissing"]
        ].merge(
            returns[["Date", "Strategy", "Ror_less_Bmark"]],
            how="left",
            left_on=["Date", "Strategy"],
            right_on=["Date", "Strategy"],
        )
        df_alloc_ror["Ctr"] = (
            df_alloc_ror.Allocation * df_alloc_ror.Ror_less_Bmark
        )

        ctr = self.calc_ctr(
            df_alloc_ror.pivot_table(
                index="Date",
                columns=["Strategy", "GcmMissing"],
                values="Ctr",
            )
            .fillna(0)
            .loc[start_date:end_date]
        ).reset_index()
        ctr_df = (
            ctr.pivot_table(
                index="Strategy", columns="GcmMissing", values="CTR"
            )
            .reindex(self._sub_strategy_order)
            .rename(
                columns={
                    "Selection": "StrategySelection",
                    "Sizing": "StrategySizing",
                }
            )
        )

        return ctr_df

    def get_ctr_rpt(
        self, gcm, bmark, start_date, end_date, trailing_period
    ):
        gcm_alloc = (
            gcm[["Date", "Strategy", "pct_strategy_of_portfolio_total"]]
            .drop_duplicates()
            .pivot(
                index="Date",
                columns="Strategy",
                values="pct_strategy_of_portfolio_total",
            )
        )
        gcm_cap_ror = self.get_returns_from_allocs(
            df=gcm,
            group_cols=["Date", "Strategy"],
            multiply_x="Ror",
            multiply_y="pct_investment_of_portfolio_strategy_total",
        )
        gcm_ror_pivot = gcm_cap_ror.pivot(
            index="Date", columns="Strategy", values="Ror"
        )
        gcm_mtd_ctr = gcm_ror_pivot * gcm_alloc
        if start_date not in gcm_mtd_ctr.index:
            gcm_ctr_stat = pd.DataFrame({"CTR": [None]})
            gcm_ctr_stat = gcm_ctr_stat.reindex(self._strategy_order)
        else:
            gcm_ctr_stat = self.calc_ctr(
                gcm_mtd_ctr.fillna(0).loc[start_date:end_date]
            ).reindex(self._strategy_order)

        bmark_alloc = (
            bmark[["Date", "Strategy", "pct_strategy_of_total"]]
            .drop_duplicates()
            .pivot(
                index="Date",
                columns="Strategy",
                values="pct_strategy_of_total",
            )
        )
        bmark_eq_ror = self.get_returns_from_allocs(
            df=bmark, group_cols=["Date", "Strategy"], equal_weight=True
        )
        bmark_ror_pivot = bmark_eq_ror.pivot(
            index="Date", columns="Strategy", values="Ror"
        )
        bmark_mtd_ctr = bmark_ror_pivot * bmark_alloc
        bmark_ctr_stat = self.calc_ctr(
            bmark_mtd_ctr.fillna(0).loc[start_date:end_date]
        ).reindex(self._strategy_order)

        result = gcm_ctr_stat.merge(
            bmark_ctr_stat, left_index=True, right_index=True
        )
        result[result.columns] = np.where(
            result[result.columns] == 0, None, result[result.columns]
        )
        return result

    def get_standalone_rtn_rpt(
        self, gcm, bmark, start_date, end_date, trailing_period
    ):
        gcm_cap_ror = self.get_returns_from_allocs(
            df=gcm,
            group_cols=["Date", "Strategy"],
            multiply_x="Ror",
            multiply_y="pct_investment_of_portfolio_strategy_total",
        )

        gcm_cap_stat = (
            self.ann_return(
                gcm_cap_ror.pivot_table(
                    index="Date", columns="Strategy", values="Ror"
                )
                .fillna(0)
                .loc[start_date:end_date],
                trailing_periods=[trailing_period],
                freq=12,
            )
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )

        gcm_eq_ror = self.get_returns_from_allocs(
            df=gcm, group_cols=["Date", "Strategy"], equal_weight=True
        )
        gcm_eql_stat = (
            self.ann_return(
                gcm_eq_ror.pivot_table(
                    index="Date", columns="Strategy", values="Ror"
                )
                .fillna(0)
                .loc[start_date:end_date],
                trailing_periods=[trailing_period],
                freq=12,
            )
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )

        bmark_eq_ror = self.get_returns_from_allocs(
            df=bmark, group_cols=["Date", "Strategy"], equal_weight=True
        )
        bmark_eql_stat = (
            self.ann_return(
                bmark_eq_ror.pivot_table(
                    index="Date", columns="Strategy", values="Ror"
                )
                .fillna(0)
                .loc[start_date:end_date],
                trailing_periods=[trailing_period],
                freq=12,
            )
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )

        result = gcm_cap_stat.merge(
            gcm_eql_stat, left_index=True, right_index=True
        ).merge(bmark_eql_stat, left_index=True, right_index=True)
        result[result.columns] = np.where(
            result[result.columns] == 0, None, result[result.columns]
        )
        return result

    def get_allocation_rpt(self, gcm, bmark, start_date, end_date):
        gcm_alloc_start = (
            gcm[gcm.Date == start_date][
                ["Strategy", "pct_strategy_of_portfolio_total"]
            ]
            .rename(
                columns={
                    "pct_strategy_of_portfolio_total": "GcmAllocationStart"
                }
            )
            .drop_duplicates()
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )
        bmark_alloc_start = (
            bmark[bmark.Date == start_date][
                ["Strategy", "pct_strategy_of_total"]
            ]
            .rename(columns={"pct_strategy_of_total": "EhAllocationStart"})
            .drop_duplicates()
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )
        gcm_alloc_end = (
            gcm[gcm.Date == end_date][
                ["Strategy", "pct_strategy_of_portfolio_total"]
            ]
            .rename(
                columns={
                    "pct_strategy_of_portfolio_total": "GcmAllocationEnd"
                }
            )
            .drop_duplicates()
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )
        bmark_alloc_end = (
            bmark[
                bmark.Date
                == dt.date(
                    self._report_date.year, self._report_date.month - 1, 1
                )
            ][["Strategy", "pct_strategy_of_total"]]
            .rename(columns={"pct_strategy_of_total": "EhAllocationEnd"})
            .drop_duplicates()
            .set_index("Strategy")
            .reindex(self._strategy_order)
        )
        result = (
            gcm_alloc_start.merge(
                bmark_alloc_start, left_index=True, right_index=True
            )
            .merge(gcm_alloc_end, left_index=True, right_index=True)
            .merge(bmark_alloc_end, left_index=True, right_index=True)
        )
        return result

    def create_ts(self, df, idx, col, val):
        df_group = df.groupby([idx, col]).sum()[val].reset_index()
        pivot = (
            df_group.pivot(index=idx, columns=col, values=val)
            .resample("MS")
            .sum()
        )
        total_alloc = pivot.sum(axis=1)

        return pivot.divide(total_alloc, axis=0)

    def ann_return(
        self, returns, trailing_periods, freq=250, return_NoObs=False
    ):
        result = pd.DataFrame()
        returns = returns.dropna()
        if (
            len(trailing_periods) == 1
            and trailing_periods[0] != "Incep"
            and len(returns) < trailing_periods[0]
        ):
            result = pd.DataFrame(
                {
                    returns.columns.name: [None],
                    "AnnRor": [None],
                    "NoObs": trailing_periods,
                }
            )
            if return_NoObs:
                return result
            else:
                return result.drop(columns=["NoObs"])

        for trailing_period in trailing_periods:
            if trailing_period != "Incep":
                if len(returns) < trailing_period:
                    continue
                else:
                    return_sub = returns.tail(trailing_period)
            else:
                return_sub = returns
            if len(return_sub) <= 12:
                ann_return = pd.DataFrame(
                    pd.DataFrame((1 + return_sub).prod() - 1)
                )
            else:
                ann_return = pd.DataFrame(
                    return_sub.add(1).prod() ** (freq / len(return_sub))
                    - 1
                )
            ann_return["NoObs"] = len(return_sub)
            result = pd.concat([result, ann_return])
        result = result.reset_index().rename(
            columns={"index": "Name", 0: "AnnRor"}
        )
        if return_NoObs:
            return result
        else:
            return result.drop(columns=["NoObs"])

    def get_returns_from_allocs(
        self,
        df=None,
        group_cols=None,
        multiply_x=None,
        multiply_y=None,
        equal_weight=False,
    ):
        # example
        # group_cols = ['Acronym', 'Strategy']
        # multiply_x = 'Ror'
        # multiply_y = 'pct_investment_of_portfolio_strategy_total'

        if equal_weight:
            result = df.groupby(group_cols).Ror.mean().reset_index()
            return result

        else:
            df["Ctr"] = df[multiply_x] * df[multiply_y]
            result = (
                df.groupby(group_cols)
                .Ctr.sum()
                .reset_index()
                .rename(columns={"Ctr": "Ror"})
            )
            return result

    def get_allocation_data_portfolio(self, gcm=None, bmark=None):
        # TODO DT: separate GCM vs benchmark by consolidating function
        if gcm is None:
            gcm = self.get_ars_constituent_data_portfolio()
        if bmark is None:
            bmark = self.get_eh_constituent_data()

        gcm_total_aum = (
            gcm.groupby("Date")
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollar"})
        )
        gcm_df = gcm.merge(
            gcm_total_aum, how="left", left_on="Date", right_on="Date"
        )

        gcm_portfolio_total_aum = (
            gcm_df.groupby(["Date", "Acronym"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarPortfolio"})
        )
        gcm_df = gcm_df.merge(
            gcm_portfolio_total_aum,
            how="left",
            left_on=["Date", "Acronym"],
            right_on=["Date", "Acronym"],
        )

        gcm_strategy_aum = (
            gcm_df.groupby(["Date", "Strategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarStrategy"})
        )
        gcm_df = gcm_df.merge(
            gcm_strategy_aum,
            how="left",
            left_on=["Date", "Strategy"],
            right_on=["Date", "Strategy"],
        )

        gcm_portfolio_strategy_aum = (
            gcm_df.groupby(["Date", "Acronym", "Strategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(
                columns={"OpeningBalance": "TotalDollarPortfolioStrategy"}
            )
        )
        gcm_df = gcm_df.merge(
            gcm_portfolio_strategy_aum,
            how="left",
            left_on=["Date", "Acronym", "Strategy"],
            right_on=["Date", "Acronym", "Strategy"],
        )
        # substrat
        gcm_sub_strategy_aum = (
            gcm_df.groupby(["Date", "SubStrategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarSubStrategy"})
        )
        gcm_df = gcm_df.merge(
            gcm_sub_strategy_aum,
            how="left",
            left_on=["Date", "SubStrategy"],
            right_on=["Date", "SubStrategy"],
        )

        gcm_portfolio_sub_strategy_aum = (
            gcm_df.groupby(["Date", "Acronym", "SubStrategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(
                columns={
                    "OpeningBalance": "TotalDollarPortfolioSubStrategy"
                }
            )
        )
        gcm_df = gcm_df.merge(
            gcm_portfolio_sub_strategy_aum,
            how="left",
            left_on=["Date", "Acronym", "SubStrategy"],
            right_on=["Date", "Acronym", "SubStrategy"],
        )

        # 1st degree
        gcm_df["pct_investment_of_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollar
        )
        gcm_df["pct_investment_of_portfolio_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolio
        )
        gcm_df["pct_investment_of_substrategy_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarSubStrategy
        )
        gcm_df["pct_investment_of_strategy_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarStrategy
        )
        gcm_df["pct_investment_of_portfolio_substrategy_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolioSubStrategy
        )
        gcm_df["pct_investment_of_portfolio_strategy_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolioStrategy
        )

        # second degree
        gcm_df["pct_strategy_of_total"] = (
            gcm_df.TotalDollarStrategy / gcm_df.TotalDollar
        )
        gcm_df["pct_substrategy_of_total"] = (
            gcm_df.TotalDollarSubStrategy / gcm_df.TotalDollar
        )
        gcm_df["pct_substrategy_of_portfolio_total"] = (
            gcm_df.TotalDollarPortfolioSubStrategy
            / gcm_df.TotalDollarPortfolio
        )
        gcm_df["pct_strategy_of_portfolio_total"] = (
            gcm_df.TotalDollarPortfolioStrategy
            / gcm_df.TotalDollarPortfolio
        )

        # TODO: DT note: remove the eureka part to separate benchmark function
        # eh
        bmark_total_aum = (
            bmark.groupby("Date")
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollar"})
        )
        bmark_df = bmark.merge(
            bmark_total_aum, how="left", left_on="Date", right_on="Date"
        )

        bmark_strategy_aum = (
            bmark_df.groupby(["Date", "Strategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarStrategy"})
        )
        bmark_df = bmark_df.merge(
            bmark_strategy_aum,
            how="left",
            left_on=["Date", "Strategy"],
            right_on=["Date", "Strategy"],
        )

        bmark_sub_strategy_aum = (
            bmark_df.groupby(["Date", "SubStrategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarSubStrategy"})
        )
        bmark_df = bmark_df.merge(
            bmark_sub_strategy_aum,
            how="left",
            left_on=["Date", "SubStrategy"],
            right_on=["Date", "SubStrategy"],
        )

        bmark_df["pct_investment_of_total"] = (
            bmark_df.OpeningBalance / bmark_df.TotalDollar
        )
        bmark_df["pct_investment_of_substrategy_total"] = (
            bmark_df.OpeningBalance / bmark_df.TotalDollarSubStrategy
        )
        bmark_df["pct_investment_of_strategy_total"] = (
            bmark_df.OpeningBalance / bmark_df.TotalDollarStrategy
        )

        # second degree
        bmark_df["pct_strategy_of_total"] = (
            bmark_df.TotalDollarStrategy / bmark_df.TotalDollar
        )
        bmark_df["pct_substrategy_of_total"] = (
            bmark_df.TotalDollarSubStrategy / bmark_df.TotalDollar
        )

        # Betas - uncomment if beta buckets are wanted
        # if start_date is None:
        #     start_date = dt.date(1900, 1, 1)
        # if end_date is None:
        #     end_date = dt.date(2099, 12, 31)
        # y3 = start_date - relativedelta(months=12 * 3 - 1)
        # gcm_ret = self.get_gcm_returns(y3, end_date)
        # gcm_betas = self.calc_beta(gcm_ret, y3, end_date)
        #
        # ehi_ret = self.get_eh_returns(y3, end_date)
        # ehi_betas = self.calc_beta(ehi_ret, y3, end_date)
        #
        # gcm_df['Beta'] = gcm_df['InvestmentGroupName'].map(gcm_betas.to_dict())
        # eh_df['Beta'] = eh_df['InvestmentGroupname'].map(ehi_betas.to_dict())
        #
        # gcm_df = self.calc_beta_bucket(gcm_df)
        # eh_df = self.calc_beta_bucket(eh_df)
        #
        # gcm_beta_aum = gcm_df.groupby(['Date', 'Beta Bucket']).OpeningBalance.sum().reset_index().rename(
        #     columns={'OpeningBalance': 'TotalDollarBetaBucket'})
        # gcm_df = gcm_df.merge(gcm_beta_aum, how='left', left_on=['Date', 'Beta Bucket'],
        #                       right_on=['Date', 'Beta Bucket'])
        #
        # gcm_portfolio_beta_aum = gcm_df.groupby(['Date', 'Acronym', 'Beta Bucket']).OpeningBalance.sum().reset_index().rename(
        #     columns={'OpeningBalance': 'TotalDollarPortfolioBetaBucket'})
        # gcm_df = gcm_df.merge(gcm_portfolio_beta_aum, how='left', left_on=['Date', 'Acronym', 'Beta Bucket'],
        #                       right_on=['Date', 'Acronym', 'Beta Bucket'])
        # gcm_df['pct_investment_of_beta_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarBetaBucket
        # gcm_df['pct_investment_of_portfolio_beta_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolioBetaBucket
        #
        # gcm_df['pct_beta_of_total'] = gcm_df.TotalDollarBetaBucket/ gcm_df.TotalDollar
        # gcm_df['pct_beta_of_portfolio_total'] = gcm_df.TotalDollarPortfolioBetaBucket / gcm_df.TotalDollarPortfolio
        #
        # eh_beta_aum = eh_df.groupby(['Date', 'Beta Bucket']).OpeningBalance.sum().reset_index().rename(
        #     columns={'OpeningBalance': 'TotalDollarBetaBucket'})
        # eh_df = eh_df.merge(eh_beta_aum, how='left', left_on=['Date', 'Beta Bucket'],
        #                     right_on=['Date', 'Beta Bucket'])
        # eh_df['pct_investment_of_beta_total'] = eh_df.OpeningBalance / eh_df.TotalDollarBetaBucket
        # eh_df['pct_beta_of_total'] = eh_df.TotalDollarBetaBucket / eh_df.TotalDollar

        return gcm_df, bmark_df

    def calc_allocs(self, df):
        gcm = df[df.OpeningBalance >= 5e6]
        gcm_total_aum = (
            gcm.groupby("Date")
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollar"})
        )
        gcm_df = gcm.merge(
            gcm_total_aum, how="left", left_on="Date", right_on="Date"
        )

        gcm_strategy_aum = (
            gcm_df.groupby(["Date", "Strategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarStrategy"})
        )
        gcm_df = gcm_df.merge(
            gcm_strategy_aum,
            how="left",
            left_on=["Date", "Strategy"],
            right_on=["Date", "Strategy"],
        )

        gcm_substrategy_aum = (
            gcm_df.groupby(["Date", "SubStrategy"])
            .OpeningBalance.sum()
            .reset_index()
            .rename(columns={"OpeningBalance": "TotalDollarSubStrategy"})
        )
        gcm_df = gcm_df.merge(
            gcm_substrategy_aum,
            how="left",
            left_on=["Date", "SubStrategy"],
            right_on=["Date", "SubStrategy"],
        )

        # 1st degree
        gcm_df["pct_investment_of_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollar
        )
        gcm_df["pct_investment_of_substrategy_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarSubStrategy
        )
        gcm_df["pct_investment_of_strategy_total"] = (
            gcm_df.OpeningBalance / gcm_df.TotalDollarStrategy
        )

        # second degree
        gcm_df["pct_strategy_of_total"] = (
            gcm_df.TotalDollarStrategy / gcm_df.TotalDollar
        )
        gcm_df["pct_substrategy_of_total"] = (
            gcm_df.TotalDollarSubStrategy / gcm_df.TotalDollar
        )

        return gcm_df

    def get_allocation_data_firmwide(self):
        gcm_all = self.get_ars_constituent_data_firmwide()
        gcm_all_rslt = self.calc_allocs(gcm_all)

        gcm_multistrat = self.get_ars_constituent_data_firmwide(
            included_mandates=["Multi-Strategy"]
        )
        gcm_multistrat_rslt = self.calc_allocs(gcm_multistrat)
        return gcm_all_rslt, gcm_multistrat_rslt

    def get_substrat_map(self):
        blob_loc = AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="underlyingdata",
            path=["bba", "SubStrategyPeerMap.csv"],
        )
        params = AzureDataLakeDao.create_blob_params(blob_loc)
        file: AzureDataLakeFile = self._runner.execute(
            params=params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(params),
        )
        sub_strat_map = file.to_tabular_data(
            TabularDataOutputTypes.PandasDataFrame, params
        )
        return sub_strat_map

    def get_eh_constituent_data(self):
        eh_allocs = self.get_eh_with_inv_group()
        default_peer = (
            self._strategy_benchmark.get_default_peer_benchmarks(
                investment_group_names=None
            )[
                [
                    "InvestmentGroupName",
                    "ReportingPeerGroup",
                    "BenchmarkStrategy",
                    "BenchmarkSubStrategy",
                ]
            ]
        )

        eh_default_peer = eh_allocs.merge(
            default_peer,
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        )
        substrat_map = self.get_substrat_map()
        # substrat_map = pd.read_csv(
        #     os.path.dirname(__file__)
        #     + "/input_data/"
        #     + "SubStrategyPeerMap.csv"
        # )
        eh_strategies = eh_default_peer.merge(
            substrat_map,
            how="left",
            left_on="EurekaStrategy",
            right_on="InvestmentSubStrategy",
        )
        eh_strategies["BenchmarkStrategy"] = np.where(
            eh_strategies.BenchmarkStrategy.isnull(),
            eh_strategies.StrategyOverride,
            eh_strategies.BenchmarkStrategy,
        )
        eh_strategies["BenchmarkSubStrategy"] = np.where(
            eh_strategies.BenchmarkSubStrategy.isnull(),
            eh_strategies.SubStrategyOverride,
            eh_strategies.BenchmarkSubStrategy,
        )
        eh_strategies = eh_strategies[
            eh_strategies.BenchmarkStrategy != "Other"
        ]
        eh_strategies.rename(
            columns={
                "BenchmarkStrategy": "Strategy",
                "BenchmarkSubStrategy": "SubStrategy",
            },
            inplace=True,
        )

        eh_strategies["OpeningBalance"] = 100
        result = eh_strategies[
            [
                "Date",
                "Strategy",
                "SubStrategy",
                "InvestmentGroupName",
                "OpeningBalance",
                "Ror",
            ]
        ].drop_duplicates()

        result.Date = pd.to_datetime(result.Date).dt.date
        assert len(result[result.Strategy.isnull()]) == 0
        return result

    def get_eh_with_inv_group(self):
        eh = self._strategy_benchmark.get_eurekahedge_constituents(
            benchmarks_names=["Eurekahedge Institutional 200"],
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date,
        ).rename(
            columns={
                "Strategy": "EurekaStrategy",
                "SubStrategy": "EurekaSubStrategy",
            }
        )
        # eh_groups = self.get_eh_inv_groups()
        eh_groups = self._entity_master.get_investment_entities(
            source_names=["Eurekahedge.Mdb"]
        )[["SourceInvestmentId", "InvestmentGroupName"]].drop_duplicates(
            "SourceInvestmentId"
        )

        eh_joined = eh.merge(
            eh_groups,
            how="left",
            left_on="SourceInvestmentId",
            right_on="SourceInvestmentId",
        )
        eh_joined.InvestmentGroupName = np.where(
            eh_joined.InvestmentGroupName.isnull(),
            eh_joined.InvestmentName,
            eh_joined.InvestmentGroupName,
        )

        assert len(eh_joined) == len(eh)
        # assert len(eh_joined[eh_joined.InvestmentGroupName.isnull()]) == 0
        return eh_joined

    def get_ars_constituent_data_firmwide(self, included_mandates=None):
        gcm_balances = self.get_ars_firmwide_balances_with_peer_group(
            included_mandates=included_mandates
        )

        # where multiple ROR for investment group/date/acronym; takes ROR from largest balance
        result = gcm_balances.sort_values(
            "OpeningBalance", ascending=False
        ).drop_duplicates(
            [
                "Date",
                "InvestmentGroupName",
                "OpeningBalance",
                "Strategy",
                "SubStrategy",
                "Pnl",
            ]
        )
        result["Ror"] = gcm_balances.Pnl / gcm_balances.OpeningBalance
        result.Ror = result.Ror.fillna(0)
        result.OpeningBalance = result.OpeningBalance.fillna(0)

        result.Date = pd.to_datetime(result.Date).dt.date
        result = result[~result.Strategy.isnull()]
        # ensure no dups, ensure all is mapped
        # assert (len(result) == len(gcm_balances))
        # assert (len(result[result.Strategy.isnull()]) == 0)
        return result

    def get_ars_constituent_data_portfolio(self):
        gcm_balances = self.get_ars_portfolio_balances_with_peer_group()

        # where multiple ROR for investment group/date/acronym; takes ROR from largest balance
        result = gcm_balances.sort_values(
            "OpeningBalance", ascending=False
        ).drop_duplicates(
            [
                "Date",
                "Acronym",
                "InvestmentGroupName",
                "OpeningBalance",
                "Strategy",
                "SubStrategy",
                "Pnl",
            ]
        )
        result["Ror"] = gcm_balances.Pnl / gcm_balances.OpeningBalance
        result.Ror = result.Ror.fillna(0)
        result.OpeningBalance = result.OpeningBalance.fillna(0)

        result.Date = pd.to_datetime(result.Date).dt.date

        result = result[~result.Strategy.isnull()]

        # ensure no dups, ensure all is mapped
        # assert (len(result) == len(gcm_balances))
        # assert (len(result[result.Strategy.isnull()]) == 0)
        return result

    def map_gcm_strategies_to_peer_group(self, gcm_allocs):
        default_peer = (
            self._strategy_benchmark.get_default_peer_benchmarks(
                investment_group_names=None
            )[
                [
                    "InvestmentGroupName",
                    "ReportingPeerGroup",
                    "BenchmarkStrategy",
                    "BenchmarkSubStrategy",
                ]
            ]
        )
        substrat_map = self.get_substrat_map()
        # substrat_map = pd.read_csv(
        #     os.path.dirname(__file__)
        #     + "/input_data/"
        #     + "SubStrategyPeerMap.csv"
        # )

        gcm_default_peer = gcm_allocs.merge(
            default_peer,
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        ).merge(
            substrat_map,
            how="left",
            left_on="SubStrategy",
            right_on="InvestmentSubStrategy",
        )
        gcm_default_peer["BenchmarkStrategy"] = np.where(
            gcm_default_peer.BenchmarkStrategy.isnull(),
            gcm_default_peer.StrategyOverride,
            gcm_default_peer.BenchmarkStrategy,
        )
        gcm_default_peer["BenchmarkSubStrategy"] = np.where(
            gcm_default_peer.BenchmarkSubStrategy.isnull(),
            gcm_default_peer.SubStrategyOverride,
            gcm_default_peer.BenchmarkSubStrategy,
        )
        gcm_default_peer = gcm_default_peer[
            gcm_default_peer.BenchmarkStrategy != "Other"
        ]
        gcm_default_peer.drop(
            columns=[
                "StrategyOverride",
                "SubStrategyOverride",
                "InvestmentSubStrategy",
                "SubStrategy",
            ],
            inplace=True,
        )
        gcm_default_peer.rename(
            columns={
                "BenchmarkStrategy": "Strategy",
                "BenchmarkSubStrategy": "SubStrategy",
            },
            inplace=True,
        )

        return gcm_default_peer

    def get_ars_firmwide_balances_with_peer_group(
        self, included_mandates=None
    ):
        gcm_allocs = self._investment_group.get_firmwide_allocation(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date,
            included_mandates=included_mandates,
        )
        gcm_strategy_balance = self.map_gcm_strategies_to_peer_group(
            gcm_allocs.drop(columns=["Strategy"])
        )

        ###
        result = gcm_strategy_balance[
            [
                "Date",
                "InvestmentGroupName",
                "InvestmentGroupId",
                "OpeningBalance",
                "Strategy",
                "SubStrategy",
                "Pnl",
            ]
        ].drop_duplicates()

        result = result[
            (~result.OpeningBalance.isnull())
            & (result.OpeningBalance != 0)
        ]
        return result

    def get_ars_portfolio_balances_with_peer_group(self):
        gcm_allocs = self._portfolio.get_holdings(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date,
            lookthrough="False",
        ).drop(columns=["Strategy"])
        gcm_strategy_balance = self.map_gcm_strategies_to_peer_group(
            gcm_allocs
        )

        ###
        result = gcm_strategy_balance[
            [
                "Date",
                "Acronym",
                "InvestmentGroupName",
                "InvestmentGroupId",
                "OpeningBalance",
                "Strategy",
                "SubStrategy",
                "Pnl",
            ]
        ].drop_duplicates()

        result = result[
            (~result.OpeningBalance.isnull())
            & (result.OpeningBalance != 0)
        ]
        return result

    def calc_ctr(self, df):
        result = pd.DataFrame(columns=["CTR"], index=df.columns)
        rors = df.sum(axis=1)

        for col in df.columns:
            ctr = df.loc[:, col].tolist()
            ror_to_date = (1 + rors).cumprod() - 1
            idx = 1
            res = ctr[0]
            for c in ctr[1:]:
                res += c * (1 + ror_to_date[idx - 1])
                idx += 1
            result.loc[col, "CTR"] = res

        return result

    def _append_total_row(self, df):
        result = df.append(df.sum(), ignore_index=True)
        result.index = self._strategy_order + ["Total"]
        # return df.append(df.sum(), ignore_index=True)
        return result

    def _get_all_portfolio_excess_returns(self, df):
        daily_arb_returns = InvestmentGroup(
            investment_group_ids=df.InvestmentGroupId.astype("int")
            .drop_duplicates()
            .to_list()
        ).get_absolute_benchmark_returns(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date + MonthEnd(1),
        )
        monthly_arb_returns = AggregateFromDaily().transform(
            data=daily_arb_returns,
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True,
        )
        arb_bmark_rtn = monthly_arb_returns.reset_index().melt(
            var_name="InvestmentGroupId",
            value_name="BmarkRor",
            id_vars=["Date"],
        )
        eh_benchmark = self._strategy_benchmark.get_default_peer_benchmarks(
            investment_group_names=df.InvestmentGroupName.drop_duplicates().to_list()
        )[
            ["InvestmentGroupName", "EurekahedgeBenchmark"]
        ]
        eh_benchmark = (
            df[["InvestmentGroupName", "InvestmentGroupId"]]
            .drop_duplicates()
            .merge(
                eh_benchmark,
                how="left",
                left_on="InvestmentGroupName",
                right_on="InvestmentGroupName",
            )
            .fillna("Eurekahedge Institutional 200")
        )
        eh_index_returns = self._strategy_benchmark.get_eurekahedge_returns(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date,
            benchmarks_names=eh_benchmark.EurekahedgeBenchmark.drop_duplicates().to_list(),
        )
        eh_bmark_rtn = eh_index_returns.reset_index().melt(
            var_name="EurekahedgeBenchmark",
            value_name="BmarkRor",
            id_vars=["Date"],
        )
        eh_bmark_rtn = eh_bmark_rtn.merge(
            eh_benchmark,
            how="left",
            left_on="EurekahedgeBenchmark",
            right_on="EurekahedgeBenchmark",
        )

        result = pd.DataFrame()
        error_df = pd.DataFrame()
        for acronym in self._all_acronyms:
            print(acronym)
            try:
                port_dimn = self._portfolio_dimn[
                    self._portfolio_dimn.Acronym == acronym
                ][["Acronym", "StrategyMandate"]]
                opening_balance = (
                    self._gcm[
                        (self._gcm.Acronym == acronym)
                        & (self._gcm.Date == self._report_date)
                    ]
                    .TotalDollarPortfolio.unique()
                    .squeeze()
                    / 1e6
                )
                if opening_balance < 50:
                    continue
                port_dimn["OpeningBalance"] = opening_balance
                # get_ror
                port_rtn = Portfolio(
                    acronyms=[acronym]
                ).get_portfolio_return_by_acronym(
                    start_date=dt.date(2010, 1, 1),
                    end_date=self._report_date,
                )
                port_stats = self.ann_return(
                    port_rtn,
                    trailing_periods=list(
                        self._trailing_period_df[
                            self._trailing_period_df.Period.isin(
                                ["YTD", "TTM", "3Y", "5Y"]
                            )
                        ].TrailingPeriod
                    ),
                    freq=12,
                    return_NoObs=True,
                )
                port_df = port_dimn.merge(
                    port_stats.pivot_table(
                        columns="NoObs", values="AnnRor", index="Acronym"
                    ).reset_index()
                )

                gcm_df = self._gcm[self._gcm.Acronym == acronym]
                excess_return_arb = self.get_excess_return_rpt(
                    port_rtn=gcm_df,
                    bmark_rtn=arb_bmark_rtn,
                    dollar_size=True,
                ).head(1)
                excess_return_eh = self.get_excess_return_rpt(
                    port_rtn=gcm_df,
                    bmark_rtn=eh_bmark_rtn[
                        [
                            "Date",
                            "EurekahedgeBenchmark",
                            "InvestmentGroupId",
                            "BmarkRor",
                        ]
                    ],
                    dollar_size=True,
                ).head(1)
                # uncomment below if attribution summary is wanted
                # port_attrib_rslt = pd.DataFrame()
                # for start_date in [dt.date(self._report_date.year, 1, 1),
                #                         self._report_date - relativedelta(years=1) + relativedelta(months=1),
                #                         self._report_date - relativedelta(years=3) + relativedelta(months=1),
                #                         self._report_date - relativedelta(years=5) + relativedelta(months=1)]:
                #     portfolio_attrib = self.get_attribution_rpt(gcm=gcm_df.copy(), bmark=self._eh.copy(),
                #                                                 start_date=start_date, end_date=self._report_date, ctr=None).sum().reset_index()
                #     portfolio_attrib["Acronym"] = acronym
                #     if len(port_attrib_rslt) == 0:
                #         port_attrib_rslt = portfolio_attrib
                #         continue
                #     port_attrib_rslt = port_attrib_rslt.merge(portfolio_attrib, how='left', left_on='index', right_on='index')

                excess_return = excess_return_arb.merge(
                    excess_return_eh, left_index=True, right_index=True
                )
                excess_return["Acronym"] = acronym

                rslt = port_df.merge(excess_return)
                result = pd.concat([result, rslt])
            except Exception as e:
                error_msg = getattr(e, "message", repr(e))
                print(error_msg)
                error_df = pd.concat(
                    [
                        pd.DataFrame(
                            {
                                "Portfolio": [acronym],
                                "Date": [self._report_date],
                                "ErrorMessage": [error_msg],
                            }
                        ),
                        error_df,
                    ]
                )
        return result

    def get_report_data(self, df, bmark, level="Strategy", firm=True):
        if firm:
            df.rename(
                columns={
                    "pct_investment_of_total": "pct_investment_of_portfolio_total",
                    "pct_investment_of_strategy_total": "pct_investment_of_portfolio_strategy_total",
                    "pct_investment_of_substrategy_total": "pct_investment_of_portfolio_substrategy_total",
                    "pct_strategy_of_total": "pct_strategy_of_portfolio_total",
                    "pct_substrategy_of_total": "pct_substrategy_of_portfolio_total",
                },
                inplace=True,
            )
        if level == "SubStrategy":
            df.drop(
                columns=[
                    "pct_strategy_of_portfolio_total",
                    "Strategy",
                    "pct_investment_of_portfolio_strategy_total",
                ],
                inplace=True,
            )
            df.rename(
                columns={
                    "pct_substrategy_of_portfolio_total": "pct_strategy_of_portfolio_total",
                    "SubStrategy": "Strategy",
                    "pct_investment_of_portfolio_substrategy_total": "pct_investment_of_portfolio_strategy_total",
                },
                inplace=True,
            )
            bmark.drop(
                columns=["pct_strategy_of_total", "Strategy"], inplace=True
            )
            bmark.rename(
                columns={
                    "pct_substrategy_of_total": "pct_strategy_of_total",
                    "SubStrategy": "Strategy",
                },
                inplace=True,
            )
            self._strategy_type = level
        else:
            self._strategy_type = level

        # ytd section
        ytd_start_date = dt.date(self._report_date.year, 1, 1)
        # allocations

        ytd_allocs = self.get_allocation_rpt(
            gcm=df,
            bmark=bmark,
            start_date=ytd_start_date,
            end_date=self._report_date,
        )
        ytd_allocs = self._append_total_row(ytd_allocs)
        # get_standalone_return
        ytd_standalone_rtn = self.get_standalone_rtn_rpt(
            gcm=df,
            bmark=bmark,
            start_date=ytd_start_date,
            end_date=self._report_date,
            trailing_period=self._report_date.month,
        )
        # get_ctr
        ytd_ctr = self.get_ctr_rpt(
            gcm=df,
            bmark=bmark,
            start_date=ytd_start_date,
            end_date=self._report_date,
            trailing_period=self._report_date.month,
        )
        ytd_ctr = self._append_total_row(ytd_ctr)
        # get_attribution
        ytd_attrib = self.get_attribution_rpt(
            gcm=df.copy(),
            bmark=bmark.copy(),
            start_date=ytd_start_date,
            end_date=self._report_date,
            ctr=ytd_ctr,
        )
        ytd_attrib = self._append_total_row(ytd_attrib)
        # ttm section
        ttm_start_date = (
            self._report_date
            - relativedelta(years=1)
            + relativedelta(months=1)
        )
        # allocations
        ttm_allocs = self.get_allocation_rpt(
            gcm=df.copy(),
            bmark=bmark,
            start_date=ttm_start_date,
            end_date=self._report_date,
        )
        ttm_allocs = self._append_total_row(ttm_allocs)
        # get_standalone_return
        ttm_standalone_rtn = self.get_standalone_rtn_rpt(
            gcm=df,
            bmark=bmark,
            start_date=ttm_start_date,
            end_date=self._report_date,
            trailing_period=12,
        )
        # get_ctr
        ttm_ctr = self.get_ctr_rpt(
            gcm=df,
            bmark=bmark,
            start_date=ttm_start_date,
            end_date=self._report_date,
            trailing_period=12,
        )
        ttm_ctr = self._append_total_row(ttm_ctr)
        # get_attribution
        ttm_attrib = self.get_attribution_rpt(
            gcm=df.copy(),
            bmark=bmark.copy(),
            start_date=ttm_start_date,
            end_date=self._report_date,
            ctr=ttm_ctr,
        )
        ttm_attrib = self._append_total_row(ttm_attrib)
        # 3y section
        three_y_start_date = (
            self._report_date
            - relativedelta(years=3)
            + relativedelta(months=1)
        )
        # allocations
        three_y_allocs = self.get_allocation_rpt(
            gcm=df.copy(),
            bmark=bmark,
            start_date=three_y_start_date,
            end_date=self._report_date,
        )
        three_y_allocs = self._append_total_row(three_y_allocs)
        # get_standalone_return
        three_y_standalone_rtn = self.get_standalone_rtn_rpt(
            gcm=df,
            bmark=bmark,
            start_date=three_y_start_date,
            end_date=self._report_date,
            trailing_period=36,
        )
        # get_ctr
        three_y_ctr = self.get_ctr_rpt(
            gcm=df,
            bmark=bmark,
            start_date=three_y_start_date,
            end_date=self._report_date,
            trailing_period=36,
        )
        three_y_ctr = self._append_total_row(three_y_ctr)
        # get_attribution
        three_y_attrib = self.get_attribution_rpt(
            gcm=df.copy(),
            bmark=bmark.copy(),
            start_date=three_y_start_date,
            end_date=self._report_date,
            ctr=three_y_ctr,
        )
        three_y_attrib = self._append_total_row(three_y_attrib)

        # get excess table
        # bmark_rtn = self._strategy_benchmark.get_eurekahedge_returns(start_date=dt.date(2010, 1, 1),
        #                                                              end_date=self._report_date,
        #                                                              benchmarks_names=['Eurekahedge Institutional 200']).\
        #     rename(columns={'Eurekahedge Institutional 200': 'EHI200'})
        daily_arb_returns = InvestmentGroup(
            investment_group_ids=df.InvestmentGroupId.astype("int")
            .drop_duplicates()
            .to_list()
        ).get_absolute_benchmark_returns(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date + MonthEnd(1),
        )
        monthly_arb_returns = AggregateFromDaily().transform(
            data=daily_arb_returns,
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True,
        )
        arb_bmark_rtn = monthly_arb_returns.reset_index().melt(
            var_name="InvestmentGroupId",
            value_name="BmarkRor",
            id_vars=["Date"],
        )
        excess_return_total_arb = self.get_excess_return_rpt(
            port_rtn=df, bmark_rtn=arb_bmark_rtn, dollar_size=firm
        )

        eh_benchmark = self._strategy_benchmark.get_default_peer_benchmarks(
            investment_group_names=df.InvestmentGroupName.drop_duplicates().to_list()
        )[
            ["InvestmentGroupName", "EurekahedgeBenchmark"]
        ]
        eh_benchmark = (
            df[["InvestmentGroupName", "InvestmentGroupId"]]
            .drop_duplicates()
            .merge(
                eh_benchmark,
                how="left",
                left_on="InvestmentGroupName",
                right_on="InvestmentGroupName",
            )
            .fillna("Eurekahedge Institutional 200")
        )
        eh_index_returns = self._strategy_benchmark.get_eurekahedge_returns(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date,
            benchmarks_names=eh_benchmark.EurekahedgeBenchmark.drop_duplicates().to_list(),
        )
        eh_bmark_rtn = eh_index_returns.reset_index().melt(
            var_name="EurekahedgeBenchmark",
            value_name="BmarkRor",
            id_vars=["Date"],
        )
        eh_bmark_rtn = eh_bmark_rtn.merge(
            eh_benchmark,
            how="left",
            left_on="EurekahedgeBenchmark",
            right_on="EurekahedgeBenchmark",
        )
        excess_return_total_eh = self.get_excess_return_rpt(
            port_rtn=df,
            bmark_rtn=eh_bmark_rtn[
                [
                    "Date",
                    "EurekahedgeBenchmark",
                    "InvestmentGroupId",
                    "BmarkRor",
                ]
            ],
            dollar_size=firm,
        )

        input_data = {
            "ytd_allocs": ytd_allocs,
            "ytd_standalone_rtn": ytd_standalone_rtn,
            "ytd_ctr": ytd_ctr,
            "ytd_attrib": ytd_attrib,
            "ttm_allocs": ttm_allocs,
            "ttm_standalone_rtn": ttm_standalone_rtn,
            "ttm_ctr": ttm_ctr,
            "ttm_attrib": ttm_attrib,
            "three_y_allocs": three_y_allocs,
            "three_y_standalone_rtn": three_y_standalone_rtn,
            "three_y_ctr": three_y_ctr,
            "three_y_attrib": three_y_attrib,
            "excess_return_total_arb": excess_return_total_arb,
            "excess_return_total_eh": excess_return_total_eh,
        }
        return input_data

    def _prefix_key_dict(self, prefix, input_data):
        res = {prefix + str(key): val for key, val in input_data.items()}
        return res

    def _suffix_key_dict(self, suffix, input_data):
        res = {str(key) + suffix: val for key, val in input_data.items()}
        return res

    def _format_strat_substrat(
        self, input_data_strat, input_data_substrat
    ):
        strat_order = [1, 6, 9, 12, 14, 17, 20]
        substrat_order = [2, 3, 4, 5, 7, 8, 10, 11, 13, 15, 16, 18, 19, 21]

        def _append_ordered_items(strat_key, substrat_key, offset=0):
            strat_df = input_data_strat.get(strat_key).copy()
            strat_df["Order"] = strat_order[
                0 : (len(strat_order) + offset)
            ]
            substrat_df = input_data_substrat.get(substrat_key).copy()
            substrat_df["Order"] = substrat_order[
                0 : (len(substrat_order) + offset)
            ]
            result_df = pd.concat([strat_df, substrat_df]).sort_values(
                "Order"
            )
            result = result_df.head(20 + offset).drop(columns=["Order"])
            return result

        # allocs
        ttm_allocs = _append_ordered_items(
            "ttm_allocs", "ttm_allocs_substrat"
        )
        three_y_allocs = _append_ordered_items(
            "three_y_allocs", "three_y_allocs_substrat"
        )

        # ctr
        ttm_ctr = _append_ordered_items("ttm_ctr", "ttm_ctr_substrat")
        three_y_ctr = _append_ordered_items(
            "three_y_ctr", "three_y_ctr_substrat"
        )

        # attrib
        ttm_attrib = _append_ordered_items(
            "ttm_attrib", "ttm_attrib_substrat"
        )
        three_y_attrib = _append_ordered_items(
            "three_y_attrib", "three_y_attrib_substrat"
        )

        # standalone return
        ttm_standalone_rtn = _append_ordered_items(
            "ttm_standalone_rtn", "ttm_standalone_rtn_substrat", offset=-1
        )
        three_y_standalone_rtn = _append_ordered_items(
            "three_y_standalone_rtn",
            "three_y_standalone_rtn_substrat",
            offset=-1,
        )

        input_data = {
            "ttm_allocs": ttm_allocs,
            "ttm_standalone_rtn": ttm_standalone_rtn,
            "ttm_ctr": ttm_ctr,
            "ttm_attrib": ttm_attrib,
            "three_y_allocs": three_y_allocs,
            "three_y_standalone_rtn": three_y_standalone_rtn,
            "three_y_ctr": three_y_ctr,
            "three_y_attrib": three_y_attrib,
        }
        return input_data

    def generate_firmwide_report(self):
        input_data_all = self.get_report_data(
            self._gcm_firmwide.copy(), self._eh.copy()
        )
        input_data_all_substrat = self.get_report_data(
            self._gcm_firmwide.copy(), self._eh.copy(), level="SubStrategy"
        )
        input_data_all_substrat = self._suffix_key_dict(
            "_substrat", input_data_all_substrat
        )
        input_data_substrat_formatted = self._format_strat_substrat(
            input_data_all, input_data_all_substrat
        )
        input_data_substrat_formatted = self._suffix_key_dict(
            "_substrat", input_data_substrat_formatted
        )

        input_data_ms = self.get_report_data(
            self._gcm_firmwide_multistrat.copy(), self._eh.copy()
        )
        input_data_ms_substrat = self.get_report_data(
            self._gcm_firmwide_multistrat.copy(),
            self._eh.copy(),
            level="SubStrategy",
        )
        input_data_ms_substrat = self._suffix_key_dict(
            "_substrat", input_data_ms_substrat
        )
        input_data_ms_substrat_formatted = self._format_strat_substrat(
            input_data_ms, input_data_ms_substrat
        )
        input_data_ms_substrat_formatted = self._suffix_key_dict(
            "_ms_substrat", input_data_ms_substrat_formatted
        )
        input_data_ms = self._suffix_key_dict("_ms", input_data_ms)

        # outliers attribution
        ttm_start_date = (
            self._report_date
            - relativedelta(years=1)
            + relativedelta(months=1)
        )
        ttm_outlier_input = self.get_portfolio_bba_outliers(
            acronyms=self._all_acronyms,
            gcm=self._gcm.copy(),
            bmark=self._eh.copy(),
            start_date=ttm_start_date,
            end_date=self._report_date,
            trailing_period=self._report_date.month,
        )
        ttm_outlier_input = self._prefix_key_dict(
            "ttm_", ttm_outlier_input
        )

        three_y_start_date = (
            self._report_date
            - relativedelta(years=3)
            + relativedelta(months=1)
        )
        three_y_outlier_input = self.get_portfolio_bba_outliers(
            acronyms=self._all_acronyms,
            gcm=self._gcm.copy(),
            bmark=self._eh.copy(),
            start_date=three_y_start_date,
            end_date=self._report_date,
            trailing_period=self._report_date.month,
        )
        three_y_outlier_input = self._prefix_key_dict(
            "three_y_", three_y_outlier_input
        )

        all_portfolio_excess_ror = self._get_all_portfolio_excess_returns(
            df=self._gcm.copy()
        )
        all_portfolio_excess_ror = all_portfolio_excess_ror.sort_values(
            "OpeningBalance", ascending=False
        )

        input_data = {
            "date": pd.DataFrame(
                {
                    "Date": [
                        (
                            self._report_date.replace(day=1)
                            + dt.timedelta(days=31)
                        ).replace(day=1)
                        - dt.timedelta(days=1)
                    ]
                }
            ),
            "date_portfolios": pd.DataFrame(
                {
                    "Date": [
                        (
                            self._report_date.replace(day=1)
                            + dt.timedelta(days=31)
                        ).replace(day=1)
                        - dt.timedelta(days=1)
                    ]
                }
            ),
            "all_portfolio_excess_ror": all_portfolio_excess_ror,
        }
        input_data.update(input_data_all)
        input_data.update(input_data_ms)
        input_data.update(input_data_substrat_formatted)
        input_data.update(input_data_ms_substrat_formatted)
        input_data.update(ttm_outlier_input)
        input_data.update(three_y_outlier_input)
        return input_data

    def generate_portfolio_report(self, acronym):
        print(acronym)

        df = self._gcm[(self._gcm.Acronym == acronym)]
        port_rtn = Portfolio(
            acronyms=[acronym]
        ).get_portfolio_return_by_acronym(
            start_date=dt.date(2010, 1, 1), end_date=self._report_date
        )
        bmark_rtn = self._strategy_benchmark.get_eurekahedge_returns(
            start_date=dt.date(2010, 1, 1),
            end_date=self._report_date,
            benchmarks_names=["Eurekahedge Institutional 200"],
        ).rename(columns={"Eurekahedge Institutional 200": "EHI200"})

        # get_ror
        rtn_df = self.get_returns_rpt(port_rtn, bmark_rtn)
        # get_beta
        beta_df = self.get_betas_rpt(
            port_rtn=port_rtn,
            bmark_rtn=bmark_rtn,
            assets=[acronym, "EHI200"],
            benchmarks=["SPXT"],
        )
        # get_downside
        downside_df = self.get_downside_rpt(
            port_rtn=port_rtn,
            bmark_rtn=bmark_rtn,
            assets=[acronym, "EHI200"],
            benchmarks=["SPXT"],
        )
        # get_correlation
        correl_df = self.get_correlation_rpt(
            port_rtn=port_rtn,
            bmark_rtn=bmark_rtn,
            assets=[acronym, "EHI200"],
            benchmarks=["SPXT"],
        )
        # get_vol
        vol_df = self.get_vol_rpt(port_rtn=port_rtn, bmark_rtn=bmark_rtn)
        # get_sharpe
        sharpe_df = self.get_sharpe_rpt(
            port_rtn=port_rtn, bmark_rtn=bmark_rtn
        )

        portfolio_input_data = self.get_report_data(
            df=df.copy(),
            bmark=self._eh.copy(),
            level="Strategy",
            firm=False,
        )
        portfolio_input_data_substrat = self.get_report_data(
            df=df.copy(),
            bmark=self._eh.copy(),
            level="SubStrategy",
            firm=False,
        )
        portfolio_input_data_substrat = self._suffix_key_dict(
            "_substrat", portfolio_input_data_substrat
        )
        input_data_substrat_formatted = self._format_strat_substrat(
            portfolio_input_data, portfolio_input_data_substrat
        )
        input_data_substrat_formatted = self._suffix_key_dict(
            "_substrat", input_data_substrat_formatted
        )

        input_data = {
            "acronym_date": pd.DataFrame(
                {
                    "Name": [
                        acronym,
                        (
                            self._report_date.replace(day=1)
                            + dt.timedelta(days=31)
                        ).replace(day=1)
                        - dt.timedelta(days=1),
                    ]
                }
            ),
            "rtn_df": rtn_df,
            "beta_df": beta_df,
            "downside_df": downside_df,
            "correl_df": correl_df,
            "vol_df": vol_df,
            "sharpe_df": sharpe_df,
        }
        input_data.update(portfolio_input_data)
        input_data.update(input_data_substrat_formatted)
        return input_data

    def generate_pfund_attributes(self):
        df = self._gcm[self._gcm.Date == self._report_date][
            [
                "InvestmentGroupName",
                "InvestmentGroupId",
                "Strategy",
                "SubStrategy",
            ]
        ].drop_duplicates()
        gcm_strat = InvestmentGroup(
            investment_group_ids=df.InvestmentGroupId.astype("int")
            .drop_duplicates()
            .to_list()
        ).get_strategies()

        gcm_map = df.merge(
            gcm_strat,
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        )

        arbs = InvestmentGroup(
            investment_group_ids=df.InvestmentGroupId.astype("int")
            .drop_duplicates()
            .to_list()
        ).get_absolute_benchmarks()[
            ["InvestmentGroupName", "BenchmarkName"]
        ]

        df_arb = gcm_map.merge(
            arbs,
            how="left",
            left_on="InvestmentGroupName",
            right_on="InvestmentGroupName",
        )

        rpt_peer = self._strategy_benchmark.get_default_peer_benchmarks(
            investment_group_names=df_arb.InvestmentGroupName.drop_duplicates().to_list()
        )
        df_arb_peer = (
            df_arb[
                [
                    "InvestmentGroupName",
                    "Strategy",
                    "SubStrategy",
                    "GcmStrategy",
                    "GcmSubStrategy",
                    "BenchmarkName",
                ]
            ]
            .drop_duplicates()
            .merge(
                rpt_peer,
                how="left",
                left_on="InvestmentGroupName",
                right_on="InvestmentGroupName",
            )
        )

        result = df_arb_peer[
            ~(
                (df_arb_peer.BenchmarkName.isnull())
                | (df_arb_peer.EurekahedgeBenchmark.isnull())
            )
        ].sort_values(["Strategy", "SubStrategy", "InvestmentGroupName"])
        # result.BenchmarkName = "'" + result.BenchmarkName.astype('str')
        result = result[
            [
                "InvestmentGroupName",
                "Strategy",
                "SubStrategy",
                "EurekahedgeBenchmark",
                "ReportingPeerGroup",
                "BenchmarkName",
            ]
        ]
        input_data = {
            "attributes": result,
            "asofdate": pd.DataFrame(
                {
                    "Date": [
                        (
                            self._report_date.replace(day=1)
                            + dt.timedelta(days=31)
                        ).replace(day=1)
                        - dt.timedelta(days=1)
                    ]
                }
            ),
        }
        return input_data
