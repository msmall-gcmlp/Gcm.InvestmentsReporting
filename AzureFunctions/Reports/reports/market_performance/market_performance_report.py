import math
import pandas as pd
import datetime as dt
import numpy as np
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    ReportVertical,
    AggregateInterval,
)
from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.Scenario.scenario import Scenario
from gcm.inv.quantlib.enum_source import PeriodicROR, Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.reporting.core.reporting_runner_base import (
    ReportingRunnerBase,
)


class MarketPerformanceReport(ReportingRunnerBase):
    def __init__(
        self,
        runner,
        as_of_date,
        interval,
        factor_daily_returns,
        price_change,
        prices,
        ticker_mapping,
    ):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._interval = interval
        self._analytics = Analytics()
        self._daily_returns = factor_daily_returns
        self._daily_price_change = price_change
        self._daily_prices = prices
        self._ticker_mapping = ticker_mapping

    def _get_return_summary(self, returns, column_name, method):
        returns = returns.copy()
        asofdate = self._as_of_date

        mtd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.MTD,
            as_of_date=asofdate,
            method=method,
        )

        qtd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.QTD,
            as_of_date=asofdate,
            method=method,
        )

        ytd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.YTD,
            as_of_date=asofdate,
            method=method,
        )

        trailing_1D_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=1,
            as_of_date=asofdate,
            method=method,
            periodicity=Periodicity.Daily,
            annualize=False,
        )

        trailing_week_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=5,
            as_of_date=asofdate,
            method=method,
            periodicity=Periodicity.Daily,
            annualize=False,
        )

        trailing_1y_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=252,
            as_of_date=asofdate,
            method=method,
            periodicity=Periodicity.Daily,
            annualize=False,
        )

        # rounding to 2 so that Excess Return matches optically
        stats = [
            trailing_1D_return,
            trailing_week_return,
            mtd_return,
            qtd_return,
            ytd_return,
            trailing_1y_return,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {column_name[0]: [x if isinstance(x, float) else " " for x in stats]},
            index=["DTD", "WTD", "MTD", "QTD", "YTD", "TTM"],
        )
        return summary

    def _getr_vol_adj_move(self, returns, column_name, method):
        asofdate = self._as_of_date

        annualized_vol = self._analytics.compute_trailing_vol(
            ror=returns,
            window=504,
            as_of_date=asofdate,
            periodicity=Periodicity.Daily,
            annualize=True,
        )
        monthly_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.MTD,
            as_of_date=asofdate,
            method=method,
        )

        ytd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.YTD,
            as_of_date=asofdate,
            method=method,
        )
        # Monthly Adju
        mtd = monthly_return / ((annualized_vol * math.sqrt(asofdate.month)) / math.sqrt(12))

        ytd_vol = ytd_return / annualized_vol

        # rounding to 2 so that Excess Return matches optically
        stats = [mtd, annualized_vol, ytd_vol]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {column_name[0]: [x if isinstance(x, float) else " " for x in stats]},
            index=["MTD1", "Vol (Annlzd)", "YTD1"],
        )
        return summary

    def _max_ppt_ttm(self, returns, column, description, method):
        asofdate = self._as_of_date
        if method == "arithmetic":
            if 'Volatility' in description[0]:
                drawdown = self._analytics.compute_max_troughtopeak_on_price(
                    prices=self._daily_prices[column],
                    window=252,
                    as_of_date=asofdate,
                    periodicity=Periodicity.Daily,
                )
            else:
                drawdown = self._analytics.compute_max_drawdown_on_price(
                    prices=self._daily_prices[column],
                    window=252,
                    as_of_date=asofdate,
                    periodicity=Periodicity.Daily,
                )
        elif method == 'geometric':
            if 'Volatility' in description[0]:
                drawdown = self._analytics.compute_max_troughtopeak(
                    ror=returns,
                    window=252,
                    as_of_date=asofdate,
                    periodicity=Periodicity.Daily
                )
            else:
                drawdown = self._analytics.compute_max_drawdown(
                    ror=returns,
                    window=252,
                    as_of_date=asofdate,
                    periodicity=Periodicity.Daily
                )
        maxptt = pd.DataFrame(drawdown)
        maxptt.index = description
        maxptt.columns = ["Max PTT (TTM)"]
        return maxptt

    def get_header_info(self):
        header = pd.DataFrame({"header_info": [self._as_of_date]})
        return header

    def _out_under_preformance_vsbenchmark(
        self,
        returns,
        index,
        benchmark_return,
        method,
        period=PeriodicROR.MTD,
    ):
        asofdate = self._as_of_date

        index_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=period,
            as_of_date=asofdate,
            method=method,
        )

        benchmark_return = self._analytics.compute_periodic_return(
            ror=benchmark_return,
            period=period,
            as_of_date=asofdate,
            method=method,
        )
        dif = np.around((index_return.values - benchmark_return.values), 3)

        out_under_return = pd.DataFrame(dif, columns=["Out_Under"])
        out_under_return.index = index
        return out_under_return

    def _get_returns_by_category(self, category):
        maping = self._ticker_mapping
        tickers_subset = maping[maping["Category"] == category]
        daily_returns = self._daily_returns.copy()
        daily_returns = daily_returns[daily_returns.columns.intersection(tickers_subset.Ticker.values.tolist())]

        return daily_returns

    def _get_price_change_by_category(self, category):
        maping = self._ticker_mapping
        tickers_subset = maping[maping["Category"] == category]
        daily_price_change = self._daily_price_change.copy()
        daily_price_change = daily_price_change[
            daily_price_change.columns.intersection(tickers_subset.Ticker.values.tolist())
        ]

        return daily_price_change

    def _get_price_by_category(self, category):

        maping = self._ticker_mapping
        tickers_subset = maping[maping["Category"] == category]
        daily_price = self._daily_prices.copy()
        daily_price = daily_price[daily_price.columns.intersection(tickers_subset.Ticker.values.tolist())]

        return daily_price

    def _get_daily_last_rpice(self, category):
        price = self._get_price_by_category(category)
        maping = self._ticker_mapping
        tickers_subset = maping[maping["Category"] == category]
        mapping_dict = tickers_subset.set_index(["Ticker"])["description"].to_dict()
        price.rename(columns=mapping_dict, inplace=True)
        price = price.fillna(method="ffill")
        last_price = price.tail(1).T
        last_price.columns = ["Last"]
        return last_price

    def _general_summary(self, category, benchmark_returns=None, benchmarking=False):

        output_table = pd.DataFrame()
        returns_by_category = self._get_returns_by_category(category)
        returns_by_category = returns_by_category.fillna(method="ffill")
        price_change_by_category = self._get_price_change_by_category(category)
        price_change_by_category = price_change_by_category.fillna(method="ffill")
        combined_columns = set(returns_by_category.columns.append(price_change_by_category.columns))
        for column in combined_columns:
            transformation = self._ticker_mapping[self._ticker_mapping["Ticker"] == column].Transformation.values
            description = self._ticker_mapping[self._ticker_mapping["Ticker"] == column].description.values

            if transformation[0] == "arithmetic":
                input_returns = price_change_by_category[column]
            else:
                input_returns = returns_by_category[column]

            agg_returns = self._get_return_summary(input_returns, description, transformation[0])
            agg_vol = self._getr_vol_adj_move(input_returns, description, transformation[0])
            max_dd = self._max_ppt_ttm(input_returns, column, description, transformation[0])

            if benchmarking:
                out_under = self._out_under_preformance_vsbenchmark(
                    input_returns,
                    description,
                    benchmark_returns,
                    method=transformation[0],
                )
                agg_stat = pd.concat([out_under, agg_returns.T, agg_vol.T, max_dd], axis=1)
            else:
                agg_stat = pd.concat([agg_returns.T, agg_vol.T, max_dd], axis=1)
            # Change the unit, multiply by 100

            unit_mult100 = self._ticker_mapping[self._ticker_mapping["Ticker"] == column].Unit_Mul100.values
            unit_in_prc = self._ticker_mapping[self._ticker_mapping["Ticker"] == column].Unit_in_prct.values
            format = self._ticker_mapping[self._ticker_mapping["Ticker"] == column].format.values
            columns_transform = [
                "QTD",
                "YTD",
                "TTM",
                "Max PTT (TTM)",
                "Vol (Annlzd)",
            ]
            # to change the unit

            if unit_mult100[0]:
                function = (
                    lambda x: 100 * x
                    if (x.name in columns_transform)
                    else 100 * x
                    if (x.name in ["MTD", "DTD", "WTD"])
                    else x
                )
                agg_stat = agg_stat.apply(function)
            else:
                function = lambda x: x
                agg_stat = agg_stat.apply(function)

            # to add % sign
            if format[0]:
                if unit_in_prc[0]:
                    function = (
                        lambda x: x.astype("int").astype("str") + "%"
                        if (x.name in columns_transform)
                        else round(x, 1).astype("str") + "%"
                        if (x.name in ["MTD", "DTD", "WTD"])
                        else round(x, 1)
                    )
                    agg_stat = agg_stat.apply(function)
                else:
                    function = (
                        lambda x: x.astype("int").astype("str")
                        if (x.name in columns_transform)
                        else round(x, 1).astype("str")
                        if (x.name in ["MTD", "DTD", "WTD"])
                        else round(x, 1)
                    )
                    agg_stat = agg_stat.apply(function)

            output_table = output_table.append(agg_stat)

        return output_table

    def aggregated_stats(self, category):
        if category == "SP_Sectors":
            base_summary = self._general_summary(
                category,
                benchmark_returns=self._daily_returns["SPX Index"],
                benchmarking=True,
            )
            base_summary.reset_index(inplace=True)
            base_summary = base_summary.reindex(
                columns=[
                    "index",
                    "Out_Under",
                    "DTD",
                    "WTD",
                    "MTD",
                    "MTD1",
                    "Vol (Annlzd)",
                    "QTD",
                    "YTD",
                    "YTD1",
                    "TTM",
                    "Max PTT (TTM)",
                ]
            )

        elif category == "L_S_Equity_Styles" or category == "L_S_Equity_Industry_Factors":
            base_summary = self._general_summary(category)
            base_summary = pd.merge(
                self._ticker_mapping[["family", "description"]],
                base_summary.reset_index(),
                how="right",
                left_on="description",
                right_on="index",
            )
            base_summary.drop(columns="description", inplace=True)
            base_summary.set_index("index", inplace=True)
            if category == "L_S_Equity_Industry_Factors":
                # TODO: subset dynamically based on the target threshold
                base_summary["MTD1abs"] = abs(base_summary["MTD1"])
                base_summary = base_summary.sort_values(by=["MTD1abs"], ascending=False)
                base_summary = base_summary.iloc[0:30]
                base_summary.drop(columns=["MTD1abs"], inplace=True)

            base_summary.reset_index(inplace=True)
            base_summary = base_summary.reindex(
                columns=[
                    "index",
                    "family",
                    "DTD",
                    "WTD",
                    "MTD",
                    "MTD1",
                    "Vol (Annlzd)",
                    "QTD",
                    "YTD",
                    "YTD1",
                    "TTM",
                    "Max PTT (TTM)",
                ]
            )

        else:

            base_summary = self._general_summary(category)
            last_price = self._get_daily_last_rpice(category)
            base_summary = pd.merge(base_summary, last_price, left_index=True, right_index=True)
            base_summary.reset_index(inplace=True)
            base_summary = base_summary.reindex(
                columns=[
                    "index",
                    "Last",
                    "DTD",
                    "WTD",
                    "MTD",
                    "MTD1",
                    "Vol (Annlzd)",
                    "QTD",
                    "YTD",
                    "YTD1",
                    "TTM",
                    "Max PTT (TTM)",
                ]
            )
        base_summary = base_summary.sort_values(by=["MTD1"], ascending=False)
        return base_summary

    def generate_market_performance_quality_report(self):
        header_info = self.get_header_info()
        global_equities = self.aggregated_stats("Global Equities")
        sp_sector = self.aggregated_stats("SP_Sectors")
        commodities = self.aggregated_stats("Commodities")
        credit_bps = self.aggregated_stats("Credit_bps")
        credit_perc = self.aggregated_stats("Credit_Percent")
        rates = self.aggregated_stats("Rates")
        currencies = self.aggregated_stats("Currencies")
        ls_equity_styles = self.aggregated_stats("L_S_Equity_Styles")
        ls_industry_factors = self.aggregated_stats("L_S_Equity_Industry_Factors")
        volatility = self.aggregated_stats("Volatility")
        input_data = {
            "header_info": header_info,
            "Global_Equities": global_equities,
            "Commodities": commodities,
            "Credit_bps": credit_bps,
            "Credit_Percent": credit_perc,
            "Rates": rates,
            "Currencies": currencies,
            "L_S_Equity_Styles": ls_equity_styles,
            "L_S_Equity_Industry_Factors": ls_industry_factors,
            "SP_Sectors": sp_sector,
            "Volatility": volatility,
        }

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="Market Performance_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_source=DaoSource.InvestmentsDwh,
                report_name="Market Performance",
                report_type=ReportType.Market,
                report_frequency="Daily",
                report_vertical=ReportVertical.FirmWide,
                aggregate_intervals=AggregateInterval.Daily,
            )

    def run(self, **kwargs):
        self.generate_market_performance_quality_report()
        return True
