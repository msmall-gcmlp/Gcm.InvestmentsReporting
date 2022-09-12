import json
import logging
import numpy as np
import pandas as pd
import datetime as dt
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
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
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.dataprovider.portfolio import Portfolio


class EofReturnBasedAttributionReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("runner"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._periodicity = Scenario.get_attribute("periodicity")
        self._analytics = Analytics()
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/eof_rba"
        self._summary_data_location = "raw/investmentsreporting/summarydata/eof_rba"

    @property
    def _start_date(self):
        if self._periodicity == PeriodicROR.ITD:
            start_date = dt.date(2020, 10, 1)
        elif self._periodicity == PeriodicROR.YTD:
            start_date = dt.date(self._as_of_date.year, 1, 1)
        return start_date

    @property
    def _end_date(self):
        return self._as_of_date

    def _get_rba_summary(self):
        subtotal_factors = [
            "SYSTEMATIC",
            "X_ASSET_CLASS",
            "PUBLIC_LS",
            "NON_FACTOR",
        ]
        non_subtotal_factors = [
            "INDUSTRY",
            "REGION",
            "LS_EQUITY_VALUE_GROUP",
            "LS_EQUITY_GROWTH_GROUP",
            "LS_EQUITY_MOMENTUM_GROUP",
            "LS_EQUITY_QUALITY_GROUP",
            "LS_EQUITY_SIZE_GROUP",
            "LS_EQUITY_RESIDUAL_VOL_GROUP",
            "LS_EQUITY_OTHER",
            "NON_FACTOR_SECURITY_SELECTION_PUBLICS",
            "NON_FACTOR_OUTLIER_EFFECTS",
        ]

        rba_subtotals = self._inv_group.get_rba_return_decomposition_by_date(
            start_date=self._start_date,
            end_date=self._end_date,
            factor_filter=subtotal_factors,
            frequency="D",
            window=36,
        )

        rba_non_subtotals = self._inv_group.get_rba_return_decomposition_by_date(
            start_date=self._start_date,
            end_date=self._end_date,
            factor_filter=non_subtotal_factors,
            frequency="D",
            window=36,
        )

        rba_summary = pd.DataFrame(
            index=[
                "SYSTEMATIC",
                "X_ASSET_CLASS",
                "INDUSTRY",
                "REGION",
                "PUBLIC_LS",
                "LS_EQUITY_VALUE_GROUP",
                "LS_EQUITY_GROWTH_GROUP",
                "LS_EQUITY_MOMENTUM_GROUP",
                "LS_EQUITY_QUALITY_GROUP",
                "LS_EQUITY_SIZE_GROUP",
                "LS_EQUITY_RESIDUAL_VOL_GROUP",
                "LS_EQUITY_OTHER",
                "NON_FACTOR",
                "NON_FACTOR_SECURITY_SELECTION_PUBLICS",
                "NON_FACTOR_OUTLIER_EFFECTS",
            ]
        )

        fund_names = rba_subtotals.columns.get_level_values(0).unique().tolist()
        # todo try rba_subtotlas.groupby(level=0, axis=1).apply(lambda x: x{x.name]).apply(lambda x: )
        for fund in fund_names:
            subtotals = rba_subtotals[fund]
            subtotals.columns = subtotals.columns.droplevel(0).droplevel(0)

            non_subtotals = rba_non_subtotals[fund]
            non_subtotals.columns = non_subtotals.columns.droplevel(0).droplevel(0)

            subtotal_decomp = self._analytics.compute_return_attributions(
                attribution_ts=subtotals,
                periodicity=Periodicity.Daily,
                as_of_date=self._end_date,
                annualize=True,
            )

            non_subtotal_decomp = self._analytics.compute_return_attributions(
                attribution_ts=non_subtotals,
                periodicity=Periodicity.Daily,
                as_of_date=self._end_date,
                annualize=True,
            )

            fund_rba = pd.concat([subtotal_decomp, non_subtotal_decomp], axis=0)

            fund_rba.rename(columns={"CTR": fund}, inplace=True)
            rba_summary = rba_summary.merge(fund_rba, left_index=True, right_index=True, how="left")

        rba_summary = rba_summary.fillna(0)
        rba_summary.index = rba_summary.index + "_RETURN_ATTRIB"
        return rba_summary

    @staticmethod
    def _get_top_line_summary(rba_summary):
        subtotal_factors = [
            "SYSTEMATIC",
            "X_ASSET_CLASS",
            "PUBLIC_LS",
            "NON_FACTOR",
        ]
        subtotal_factors = [x + "_RETURN_ATTRIB" for x in subtotal_factors]
        rba_by_subtotal = rba_summary.loc[subtotal_factors]
        ann_returns = rba_by_subtotal.sum(axis=0).to_frame()
        idios = rba_summary.loc["NON_FACTOR_SECURITY_SELECTION_PUBLICS_RETURN_ATTRIB"]
        top_line_summary = pd.concat([ann_returns, idios], axis=1)
        top_line_summary.columns = ["Return", "Idio Only"]
        top_line_summary = top_line_summary.T
        top_line_summary.index = top_line_summary.index + "_TOP_LINE"
        return top_line_summary

    def _get_risk_decomp(self):
        factors = pd.DataFrame(
            index=[
                "SYSTEMATIC",
                "X_ASSET_CLASS",
                "INDUSTRY",
                "REGION",
                "PUBLIC_LS",
                "NON_FACTOR",
            ]
        )

        decomp_fg1 = self._inv_group.get_average_risk_decomp_by_group(
            start_date=self._start_date,
            end_date=self._end_date,
            group_type="FactorGroup1",
            frequency=Periodicity.Daily.value,
            window=36,
            wide=False,
        )

        decomp_fg2 = self._inv_group.get_average_risk_decomp_by_group(
            start_date=self._start_date,
            end_date=self._end_date,
            group_type="FactorGroup2",
            frequency=Periodicity.Daily.value,
            window=36,
            wide=False,
        )

        decomp_fg1 = decomp_fg1.pivot(
            index="FactorGroup1",
            columns="InvestmentGroupName",
            values="AvgRiskContrib",
        )
        decomp_fg2 = decomp_fg2.pivot(
            index="FactorGroup2",
            columns="InvestmentGroupName",
            values="AvgRiskContrib",
        )
        decomp = pd.concat([decomp_fg1, decomp_fg2], axis=0)
        decomp = factors.merge(decomp, left_index=True, right_index=True, how="left")
        decomp = decomp.fillna(0)
        decomp.index = decomp.index + "_RISK_DECOMP"
        return decomp

    def _get_average_adj_r2(self):
        r2s = self._inv_group.get_average_adj_r2(
            start_date=self._start_date,
            end_date=self._end_date,
            frequency=Periodicity.Daily.value,
            window=36,
        )
        r2s = r2s[["InvestmentGroupName", "AvgAdjR2"]].T
        r2s.columns = r2s.loc["InvestmentGroupName"]
        r2s = r2s.drop("InvestmentGroupName")

        return r2s

    def _get_attribution_table_rba(self):
        portfolio = Portfolio()
        eof_constituents = portfolio.get_eof_constituents()
        eof_inv_group_id = portfolio.get_eof_investment_group_id()
        constituent_inv_group_ids = eof_constituents["InvestmentGroupId"].tolist()

        investment_group_ids = [eof_inv_group_id] + constituent_inv_group_ids
        self._inv_group = InvestmentGroup(investment_group_ids=investment_group_ids)

        rba_summary = self._get_rba_summary()
        top_line_summary = self._get_top_line_summary(rba_summary=rba_summary)
        risk_decomp = self._get_risk_decomp()
        r2 = self._get_average_adj_r2()
        attribution_table = pd.concat([top_line_summary, rba_summary, risk_decomp, r2])
        attribution_table = pd.concat([attribution_table.columns.to_frame().T, attribution_table])

        cols = list(attribution_table)
        cols = sorted(cols)
        cols.insert(0, cols.pop(cols.index("GCM Equity Opps Fund")))

        attribution_table = attribution_table.loc[:, cols]

        attribution_table.rename(columns={"GCM Equity Opps Fund": "EOF"}, inplace=True)

        return attribution_table

    @staticmethod
    def get_days_period_to_date(daily_returns, start_date, end_date):
        returns_within_range = (pd.to_datetime(daily_returns.index).date >= start_date) & (
            pd.to_datetime(daily_returns.index).date <= end_date
        )
        days_ptd = sum(returns_within_range)
        return days_ptd

    @staticmethod
    def calculate_median_return(returns):
        median_returns = returns.apply(np.nanmedian)
        return median_returns

    @staticmethod
    def calculate_return_percentile(returns):
        percentile_rank = returns.rank(method="max").apply(lambda x: 100 * (x - 1) / (sum(~x.isnull()) - 1))
        percentile_rank = percentile_rank.iloc[[-1]]
        percentile_rank = percentile_rank.dropna(axis=1)
        return percentile_rank.round(0).astype(int)

    @staticmethod
    def calculate_rolling_geometric_returns(returns, window, periodicity=Periodicity.Daily):
        def _get_annual_periods(periodicity):
            # when converting price levels to percent changes, need to ensure we have t-1
            # for daily, pad 7 days to ensure we capture weekends/holidays
            switcher = {
                Periodicity.Daily: 252,
                Periodicity.Monthly: 12,
                Periodicity.Quarterly: 4,
                Periodicity.Annual: 1,
            }
            return switcher.get(periodicity, "nothing")

        annual_periods = _get_annual_periods(periodicity=periodicity)
        annualizing_factor = min(1, annual_periods / window)
        rolling_returns = returns.rolling(window=window, min_periods=window).apply(
            lambda x: np.nanprod(1 + x) ** (annualizing_factor) - 1
        )
        return rolling_returns

    def _get_factor_performance_tables(self):
        factors = self._inv_group.get_rba_return_decomposition_by_date(
            start_date=self._start_date,
            end_date=self._end_date,
            frequency="D",
            window=36,
        )
        factors = factors.columns.to_frame().reset_index(drop=True)

        market_tickers = factors[factors["FactorGroup1"].isin(["SYSTEMATIC", "X_ASSET_CLASS"])]["AggLevel"].tolist()
        style_tickers = factors[factors["FactorGroup1"].isin(["PUBLIC_LS"])]["AggLevel"].tolist()
        tickers = market_tickers + style_tickers

        returns = Factor(tickers=tickers).get_returns(
            start_date=dt.date(2000, 1, 1),
            end_date=self._end_date,
            fill_na=True,
        )

        days_ptd = self.get_days_period_to_date(
            daily_returns=returns,
            start_date=self._start_date,
            end_date=self._end_date,
        )

        rolling_returns = self.calculate_rolling_geometric_returns(
            returns=returns, window=days_ptd, periodicity=Periodicity.Daily
        )

        current_ptd_return = rolling_returns.iloc[[-1]]

        current_ptd_return["Metric"] = self._periodicity.value + "TD Return (" + str(days_ptd) + " days)"

        median_returns = self.calculate_median_return(returns=rolling_returns)
        median_returns["Metric"] = "Median " + str(days_ptd) + "-day Return"

        percentiles = self.calculate_return_percentile(returns=rolling_returns)
        percentiles["Metric"] = "Ptile vs Avg (Since 2000)"

        summary = current_ptd_return.append(median_returns, ignore_index=True).append(percentiles, ignore_index=True)
        summary = summary.set_index("Metric")

        summary = summary.T
        summary = summary.sort_values(by=summary.columns[2], ascending=False)
        factors = Factor(tickers=tickers).get_factor_inventory()
        factor_hierarchy = Factor(tickers=tickers).get_factor_hierarchy()
        factors = factors.merge(
            factor_hierarchy,
            left_on="HierarchyParent",
            right_index=True,
            how="left",
        )
        factors = factors[["SourceTicker", "Description"]]
        suffix = " - Excess over MSCI ACWI - Beta Adj"
        descriptions = [x.replace(suffix, "*") if x is not None else None for x in factors["Description"].tolist()]
        factors["Description"] = descriptions

        factors.rename(columns={"Description": "Description"}, inplace=True)

        summary = summary.merge(factors, left_index=True, right_on="SourceTicker", how="left")
        # blank column for spacing in excel
        summary[""] = ""
        front_cols = ["Description", ""]
        summary = summary[front_cols + [col for col in summary.columns if col not in front_cols]]

        market_factors = summary[summary["SourceTicker"].isin(market_tickers)].drop(columns={"SourceTicker"})
        style_factors = summary[summary["SourceTicker"].isin(style_tickers)].drop(columns={"SourceTicker"})

        style_factors[" "] = ""
        front_cols = ["Description", "", " "]
        style_factors = style_factors[front_cols + [col for col in style_factors.columns if col not in front_cols]]

        market_factors = pd.concat([market_factors.columns.to_frame().T, market_factors])
        style_factors = pd.concat([style_factors.columns.to_frame().T, style_factors])

        return market_factors, style_factors

    def _write_report_to_data_lake(self, input_data, input_data_json):
        data_to_write = json.dumps(input_data_json)
        asofdate = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            "EOF_" + self._periodicity.value + "_RBA_Report_" + asofdate + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

        logging.info("JSON stored to DataLake for: " + "EOF_" + self._periodicity.value + "TD")

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="EOF RBA Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name="Equity Opps Fund Ltd",
                entity_display_name="EOF",
                entity_ids=[19163],
                entity_source=DaoSource.PubDwh,
                report_name="RBA_" + self._periodicity.value + "TD",
                report_type=ReportType.Risk,
                aggregate_intervals=AggregateInterval.MTD,
                output_dir="cleansed/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake,
            )

        logging.info("Excel stored to DataLake for: " + "EOF_" + self._periodicity.value + "TD")

    def generate_rba_report(self):
        attribution_table = self._get_attribution_table_rba()
        (
            market_factor_summary,
            style_factor_summary,
        ) = self._get_factor_performance_tables()

        input_data = {
            "attribution_table": attribution_table,
            "market_factor_summary": market_factor_summary,
            "style_factor_summary": style_factor_summary,
        }

        input_data_json = {
            "attribution_table": attribution_table.to_json(orient="index"),
            "market_factor_summary": market_factor_summary.to_json(orient="index"),
            "style_factor_summary": style_factor_summary.to_json(orient="index"),
        }
        self._write_report_to_data_lake(input_data=input_data, input_data_json=input_data_json)

    def run(self, **kwargs):
        self.generate_rba_report()
        return True
