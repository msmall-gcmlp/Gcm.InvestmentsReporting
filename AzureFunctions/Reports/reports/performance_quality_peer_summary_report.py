import json
import pandas as pd
import ast
import datetime as dt
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.quantlib.enum_source import Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import (
    AggregateFromDaily,
)
from gcm.inv.reporting.core.reporting_runner_base import (
    ReportingRunnerBase,
)


class PerformanceQualityPeerSummaryReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, peer_group):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._peer_group = peer_group
        self._analytics = Analytics()
        self._peer_returns_cache = None
        self._peer_constituent_returns_cache = None
        self._peer_inputs_cache = None
        self._market_factor_inputs_cache = None
        self._market_factor_returns_cache = None
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/performancequality"
        self._summary_data_location = "raw/investmentsreporting/summarydata/performancequality"

    def _download_inputs(self, file_name) -> dict:
        read_params = AzureDataLakeDao.create_get_data_params(
            self._underlying_data_location,
            file_name,
            retry=False,
        )
        file = self._runner.execute(
            params=read_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(read_params),
        )
        return json.loads(file.content)

    @property
    def _peer_inputs(self):
        if self._peer_inputs_cache is None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = self._peer_group.replace("/", "") + "_peer_inputs_" + asofdate + ".json"
            self._peer_inputs_cache = self._download_inputs(file_name=file)
        return self._peer_inputs_cache

    @property
    def _market_factor_inputs(self):
        if self._market_factor_inputs_cache is None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = "market_factor_returns_" + asofdate + ".json"
            self._market_factor_inputs_cache = self._download_inputs(file_name=file)
        return self._market_factor_inputs_cache

    @property
    def _gcm_peer_returns(self):
        if self._peer_returns_cache is None:
            self._peer_returns_cache = pd.read_json(self._peer_inputs["gcm_peer_returns"], orient="index")
        return self._peer_returns_cache

    @property
    def _gcm_peer_constituent_returns(self):
        if self._peer_constituent_returns_cache is None:
            returns = pd.read_json(
                self._peer_inputs["gcm_peer_constituent_returns"],
                orient="index",
            )
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(
                returns_columns,
                names=["PeerGroupName", "SourceInvestmentId"],
            )
            returns.columns = returns_columns
            self._peer_constituent_returns_cache = returns
        return self._peer_constituent_returns_cache

    @property
    def _market_factor_returns(self):
        if self._market_factor_returns_cache is None:
            returns = pd.read_json(self._market_factor_inputs, orient="index")
            if len(returns) > 0:
                returns = AggregateFromDaily().transform(
                    data=returns,
                    method="geometric",
                    period=Periodicity.Monthly,
                )
                returns.index = [dt.datetime(x.year, x.month, 1) for x in returns.index.tolist()]
                self._market_factor_returns_cache = returns
            else:
                self._market_factor_returns_cache = pd.DataFrame()
        return self._market_factor_returns_cache

    @property
    def _sp500_return(self):
        returns = self._market_factor_returns["SPXT Index"]
        returns.name = "SP500"
        return returns.to_frame()

    @property
    def _rf_return(self):
        returns = self._market_factor_returns["SBMMTB1 Index"]
        returns.name = "1M_RiskFree"
        return returns.to_frame()

    @property
    def _primary_peer_constituent_returns(self):
        peer_group_index = self._gcm_peer_constituent_returns.columns.get_level_values(0) == self._peer_group
        if any(peer_group_index):
            returns = self._gcm_peer_constituent_returns.loc[:, peer_group_index]
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
        return returns

    def _get_trailing_vol(self, returns, trailing_months):
        return self._analytics.compute_trailing_vol(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            annualize=True,
        )

    def _get_trailing_beta(self, returns, trailing_months):
        return self._analytics.compute_trailing_beta(
            ror=returns,
            benchmark_ror=self._sp500_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def _get_trailing_sharpe(self, returns, trailing_months):
        return self._analytics.compute_trailing_sharpe_ratio(
            ror=returns,
            rf_ror=self._rf_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def _get_trailing_win_loss_ratio(self, returns, trailing_months):
        return self._analytics.compute_trailing_win_loss_ratio(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def _get_trailing_batting_avg(self, returns, trailing_months):
        return self._analytics.compute_trailing_batting_average(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )

    def _get_rolling_return(self, returns, trailing_months):
        return self._analytics.compute_trailing_return(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
            include_history=True,
        )

    def _get_rolling_vol(self, returns, trailing_months):
        return self._analytics.compute_trailing_vol(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            annualize=True,
            include_history=True,
        )

    def _get_rolling_sharpe_ratio(self, returns, trailing_months):
        rolling_sharpe = self._analytics.compute_trailing_sharpe_ratio(
            ror=returns,
            rf_ror=self._rf_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )
        # outlier removal
        max_sharpe = min(10, rolling_sharpe.max().quantile(0.95))
        min_sharpe = rolling_sharpe.min().quantile(0.05)
        outlier_ind = (rolling_sharpe < min_sharpe) | (rolling_sharpe > max_sharpe)

        rolling_sharpe[outlier_ind] = None
        return rolling_sharpe

    def _get_rolling_beta(self, returns, trailing_months):
        return self._analytics.compute_trailing_beta(
            ror=returns,
            benchmark_ror=self._sp500_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

    def _get_rolling_batting_avg(self, returns, trailing_months):
        return self._analytics.compute_trailing_batting_average(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

    def _get_rolling_win_loss_ratio(self, returns, trailing_months):
        return self._analytics.compute_trailing_win_loss_ratio(
            ror=returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

    def _summarize_rolling_data(self, rolling_data, trailing_months):
        if rolling_data.index.max().date() < self._as_of_date.replace(day=1):
            rolling_data = pd.DataFrame()

        rolling_data = rolling_data.iloc[-trailing_months:]

        index = ["min", "25%", "75%", "max"]
        if len(rolling_data) == trailing_months:
            summary = rolling_data.describe().loc[index].round(2)
        else:
            summary = pd.DataFrame({"Fund": [""] * len(index)}, index=index)

        return summary

    def _summarize_rolling_median(self, rolling_data, trailing_months):
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

    def _get_peer_rolling_return_summary(self):
        returns = self._primary_peer_constituent_returns.copy()
        rolling_12m_returns = self._get_rolling_return(returns=returns, trailing_months=12)

        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=12)
        rolling_1y_summary = rolling_1y_summary.median(axis=1)

        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=36)
        rolling_3y_summary = rolling_3y_summary.median(axis=1)

        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=60)
        rolling_5y_summary = rolling_5y_summary.median(axis=1)

        summary = pd.concat(
            [rolling_1y_summary, rolling_3y_summary, rolling_5y_summary],
            axis=1,
        ).T
        summary.index = ["TTM", "3Y", "5Y"]
        summary = summary.round(2)

        return summary

    def _get_peer_rolling_sharpe_summary(self):
        returns = self._primary_peer_constituent_returns.copy()
        rolling_12m_sharpes = self._get_rolling_sharpe_ratio(returns=returns, trailing_months=12)

        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=12)
        rolling_1y_summary = rolling_1y_summary.median(axis=1)

        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=36)
        rolling_3y_summary = rolling_3y_summary.median(axis=1)

        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=60)
        rolling_5y_summary = rolling_5y_summary.median(axis=1)

        summary = pd.concat(
            [rolling_1y_summary, rolling_3y_summary, rolling_5y_summary],
            axis=1,
        ).T
        summary.index = ["TTM", "3Y", "5Y"]
        summary = summary.round(2)

        return summary

    def _get_peer_trailing_vol_summary(self, returns):
        returns = returns.copy()
        trailing_1y_vol = self._get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_vol = self._get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_vol = self._get_trailing_vol(returns=returns, trailing_months=60)
        rolling_1_vol = self._get_rolling_vol(returns=returns, trailing_months=12)
        trailing_5y_median_vol = self._summarize_rolling_median(rolling_1_vol, trailing_months=60)

        stats = [
            trailing_1y_vol.median(),
            trailing_3y_vol.median(),
            trailing_5y_vol.median(),
            trailing_5y_median_vol.median().squeeze(),
        ]
        summary = pd.DataFrame(
            {"AvgVol": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_peer_trailing_beta_summary(self, returns):
        returns = returns.copy()
        trailing_1y_beta = self._get_trailing_beta(returns=returns, trailing_months=12)
        trailing_3y_beta = self._get_trailing_beta(returns=returns, trailing_months=36)
        trailing_5y_beta = self._get_trailing_beta(returns=returns, trailing_months=60)
        rolling_1_beta = self._get_rolling_beta(returns=returns, trailing_months=12)
        trailing_5y_median_beta = self._summarize_rolling_median(rolling_1_beta, trailing_months=60)

        stats = [
            trailing_1y_beta.median(),
            trailing_3y_beta.median(),
            trailing_5y_beta.median(),
            trailing_5y_median_beta.median().squeeze(),
        ]
        summary = pd.DataFrame(
            {"AvgBeta": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_peer_trailing_sharpe_summary(self, returns):
        returns = returns.copy()
        trailing_1y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=12)
        trailing_3y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=36)
        trailing_5y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=60)
        rolling_1_sharpe = self._get_rolling_sharpe_ratio(returns=returns, trailing_months=12)
        trailing_5y_median_sharpe = self._summarize_rolling_median(rolling_1_sharpe, trailing_months=60)

        stats = [
            trailing_1y_sharpe.median(),
            trailing_3y_sharpe.median(),
            trailing_5y_sharpe.median(),
            trailing_5y_median_sharpe.median().squeeze(),
        ]
        summary = pd.DataFrame(
            {"AvgSharpe": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_peer_trailing_batting_average_summary(self, returns):
        returns = returns.copy()
        trailing_1y = self._get_trailing_batting_avg(returns=returns, trailing_months=12)
        trailing_3y = self._get_trailing_batting_avg(returns=returns, trailing_months=36)
        trailing_5y = self._get_trailing_batting_avg(returns=returns, trailing_months=60)
        rolling_1_batting = self._get_rolling_batting_avg(returns=returns, trailing_months=12)
        trailing_5y_median = self._summarize_rolling_median(rolling_1_batting, trailing_months=60)

        stats = [
            trailing_1y.median(),
            trailing_3y.median(),
            trailing_5y.median(),
            trailing_5y_median.median().squeeze(),
        ]
        summary = pd.DataFrame(
            {"AvgBattingAvg": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_peer_trailing_win_loss_ratio_summary(self, returns):
        returns = returns.copy()
        trailing_1y = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_3y = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=36)
        trailing_5y = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=60)
        rolling_1y = self._get_rolling_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_5y_median = self._summarize_rolling_median(rolling_1y, trailing_months=60)

        stats = [
            trailing_1y.median(),
            trailing_3y.median(),
            trailing_5y.median(),
            trailing_5y_median.median().squeeze(),
        ]
        summary = pd.DataFrame(
            {"AvgWinLoss": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def build_performance_stability_peer_summary(self):
        peer_returns = self._primary_peer_constituent_returns.copy()

        if peer_returns.shape[0] > 0:
            vol = self._get_peer_trailing_vol_summary(returns=peer_returns)
            beta = self._get_peer_trailing_beta_summary(returns=peer_returns)
            sharpe = self._get_peer_trailing_sharpe_summary(returns=peer_returns)
            batting_avg = self._get_peer_trailing_batting_average_summary(returns=peer_returns)
            win_loss = self._get_peer_trailing_win_loss_ratio_summary(returns=peer_returns)

            rolling_returns = self._get_peer_rolling_return_summary()
            rolling_returns.columns = ["AvgReturn_"] + rolling_returns.columns

            rolling_sharpes = self._get_peer_rolling_sharpe_summary()
            rolling_sharpes.columns = ["AvgSharpe_"] + rolling_sharpes.columns

            summary = vol.merge(beta, left_index=True, right_index=True, how="left")
            summary = summary.merge(sharpe, left_index=True, right_index=True, how="left")
            summary = summary.merge(batting_avg, left_index=True, right_index=True, how="left")
            summary = summary.merge(win_loss, left_index=True, right_index=True, how="left")
            summary = summary.merge(
                rolling_returns,
                left_index=True,
                right_index=True,
                how="left",
            )
            summary = summary.merge(
                rolling_sharpes,
                left_index=True,
                right_index=True,
                how="left",
            )

            summary = summary[
                [
                    "AvgVol",
                    "AvgBeta",
                    "AvgSharpe",
                    "AvgBattingAvg",
                    "AvgWinLoss",
                    "AvgReturn_min",
                    "AvgReturn_25%",
                    "AvgReturn_75%",
                    "AvgReturn_max",
                    "AvgSharpe_min",
                    "AvgSharpe_25%",
                    "AvgSharpe_75%",
                    "AvgSharpe_max",
                ]
            ]

        else:
            summary = pd.DataFrame(
                columns=[
                    "AvgVol",
                    "AvgBeta",
                    "AvgSharpe",
                    "AvgBattingAvg",
                    "AvgWinLoss",
                    "AvgReturn_min",
                    "AvgReturn_25%",
                    "AvgReturn_75%",
                    "AvgReturn_max",
                    "AvgSharpe_min",
                    "AvgSharpe_25%",
                    "AvgSharpe_75%",
                    "AvgSharpe_max",
                ],
                index=["TTM", "3Y", "5Y", "5YMedian"],
            )
        return summary

    def generate_performance_quality_peer_summary_report(self):
        performance_stability_peer_summary = self.build_performance_stability_peer_summary()

        input_data_json = {
            "performance_stability_peer_summary": performance_stability_peer_summary.to_json(orient="index"),
        }

        data_to_write = json.dumps(input_data_json)
        asofdate = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            self._peer_group.replace("/", "") + "_peer_" + asofdate + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

    def run(self, **kwargs):
        self.generate_performance_quality_peer_summary_report()
        return True
