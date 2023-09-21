import json
import pandas as pd
import numpy as np
import datetime as dt
from functools import cached_property
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import (
    AggregateFromDaily,
)
from gcm.inv.scenario import Scenario


class PerformanceQualityHelper:
    def __init__(self):
        self._dao = Scenario.get_attribute("dao")
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self.analytics = Analytics()
        self.underlying_data_location = "raw/investmentsreporting/underlyingdata/performancequality"

    def download_inputs(self, location, file_path) -> dict:
        read_params = AzureDataLakeDao.create_get_data_params(
            location,
            file_path,
            retry=False,
        )
        file = self._dao.execute(
            params=read_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(read_params),
        )
        inputs = json.loads(file.content)
        return inputs

    @cached_property
    def market_factor_returns_daily(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = "market_factor_returns_" + as_of_date + ".json"
        market_factor_inputs = self.download_inputs(
            location=self.underlying_data_location, file_path=file
        )

        returns = pd.read_json(market_factor_inputs, orient="index")
        return returns

    @cached_property
    def market_factor_returns(self):
        if len(self.market_factor_returns_daily) > 0:
            returns = AggregateFromDaily().transform(
                data=self.market_factor_returns_daily[["I00078US Index", "SPXT Index"]],
                method="geometric",
                period=Periodicity.Monthly,
            )
            returns.index = [dt.datetime(x.year, x.month, 1) for x in returns.index.tolist()]
            returns = returns[-120:]
        else:
            returns = pd.DataFrame()
        return returns

    @cached_property
    def peer_arb_mapping(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = "market_factor_returns_" + as_of_date + ".json"
        market_factor_inputs = self.download_inputs(
            location=self.underlying_data_location, file_path=file
        )

        returns = pd.read_json(market_factor_inputs, orient="index")
        if len(returns) > 0:
            returns = AggregateFromDaily().transform(
                data=returns,
                method="geometric",
                period=Periodicity.Monthly,
            )
            returns.index = [dt.datetime(x.year, x.month, 1) for x in returns.index.tolist()]
        else:
            returns = pd.DataFrame()
        return returns

    @cached_property
    def sp500_return(self):
        returns = self.market_factor_returns["SPXT Index"]
        returns.name = "SP500"
        return returns.to_frame()

    @cached_property
    def rf_return(self):
        returns = self.market_factor_returns["I00078US Index"]
        returns.name = "1M_RiskFree"
        return returns.to_frame()

    def get_trailing_vol(self, returns, trailing_months):
        return self.analytics.compute_trailing_vol(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            annualize=True,
        )

    def get_trailing_beta(self, returns, trailing_months):
        return self.analytics.compute_trailing_beta(
            ror=returns,
            benchmark_ror=self.sp500_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def get_trailing_sharpe(self, returns, trailing_months):
        return self.analytics.compute_trailing_sharpe_ratio(
            ror=returns,
            rf_ror=self.rf_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def get_trailing_win_loss_ratio(self, returns, trailing_months):
        return self.analytics.compute_trailing_win_loss_ratio(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def get_trailing_batting_avg(self, returns, trailing_months):
        return self.analytics.compute_trailing_batting_average(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def get_rolling_return(self, returns, trailing_months):
        return self.analytics.compute_trailing_return(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
            include_history=True,
        )

    def get_rolling_vol(self, returns, trailing_months):
        return self.analytics.compute_trailing_vol(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            annualize=True,
            include_history=True,
        )

    def get_rolling_sharpe_ratio(self, returns, trailing_months, remove_outliers=False):
        rolling_sharpe = self.analytics.compute_trailing_sharpe_ratio(
            ror=returns,
            rf_ror=self.rf_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

        if remove_outliers:
            max_sharpe = min(10, rolling_sharpe.max().quantile(0.95))
            min_sharpe = rolling_sharpe.min().quantile(0.05)
            outlier_ind = (rolling_sharpe < min_sharpe) | (rolling_sharpe > max_sharpe)
            rolling_sharpe[outlier_ind] = None

        return rolling_sharpe

    def get_rolling_beta(self, returns, trailing_months):
        return self.analytics.compute_trailing_beta(
            ror=returns,
            benchmark_ror=self.sp500_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

    def get_rolling_batting_avg(self, returns, trailing_months):
        return self.analytics.compute_trailing_batting_average(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

    def get_rolling_win_loss_ratio(self, returns, trailing_months):
        return self.analytics.compute_trailing_win_loss_ratio(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

    def summarize_rolling_data(self, rolling_data, trailing_months):
        max_date = rolling_data.index.max().date()
        max_date = dt.date(1900, 1, 1) if str(max_date) == 'NaT' else max_date
        if max_date < self._as_of_date.replace(day=1):
            rolling_data = pd.DataFrame()

        rolling_data = rolling_data.iloc[-trailing_months:]

        index = ["min", "25%", "75%", "max"]
        if len(rolling_data) == trailing_months:
            quantiles = np.quantile(rolling_data, q=[0, 0.25, 0.75, 1], axis=0).round(2)
            summary = pd.DataFrame(quantiles, index=index, columns=rolling_data.columns)
        else:
            summary = pd.DataFrame({"Fund": [""] * len(index)}, index=index)

        return summary

    def summarize_rolling_median(self, rolling_data, trailing_months):
        if len(rolling_data) == 0:
            rolling_data = pd.DataFrame()
        elif rolling_data.index.max().date() < self._as_of_date.replace(day=1):
            rolling_data = pd.DataFrame()

        rolling_data = rolling_data.iloc[-trailing_months:]

        if len(rolling_data) > 1:
            # rolling_median = rolling_data.median().round(2)
            # summary = pd.DataFrame({'Fund': rolling_median.squeeze()}, index=['Median'])
            summary = rolling_data.median().round(2).to_frame()
        else:
            summary = pd.DataFrame({"Fund": [""]}, index=["Median"])

        return summary

    @staticmethod
    def summarize_counts(returns):
        def _get_peers_with_returns_in_ttm(returns):
            return returns.notna()[-12:].any().sum()

        def _get_peers_with_current_month_return(returns):
            return returns.notna().sum(axis=1)[-1]

        if returns.shape[0] == 0:
            return [np.nan, np.nan]

        updated_constituents = _get_peers_with_current_month_return(returns)
        active_constituents = _get_peers_with_returns_in_ttm(returns)
        return [updated_constituents, active_constituents]
