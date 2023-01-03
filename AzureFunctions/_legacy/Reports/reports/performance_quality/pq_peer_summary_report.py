import json
from functools import cached_property

import pandas as pd
import ast
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.quantlib.timeseries.analytics import Analytics
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from _legacy.Reports.reports.performance_quality.pq_helper import PerformanceQualityHelper


class PerformanceQualityPeerSummaryReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, peer_group):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._peer_group = peer_group
        self._analytics = Analytics()
        self._summary_data_location = "raw/investmentsreporting/summarydata/performancequality"
        self._helper = PerformanceQualityHelper(runner=self._runner, as_of_date=self._as_of_date)

    @cached_property
    def _peer_inputs(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = self._peer_group.replace("/", "") + "_peer_inputs_" + as_of_date + ".json"
        inputs = self._helper.download_inputs(location=self._helper.underlying_data_location, file_path=file)
        return inputs

    @cached_property
    def _gcm_peer_returns(self):
        returns = pd.read_json(self._peer_inputs["gcm_peer_returns"], orient="index")
        return returns

    @cached_property
    def _gcm_peer_constituent_returns(self):
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
        return returns

    @property
    def _primary_peer_constituent_returns(self):
        peer_group_index = self._gcm_peer_constituent_returns.columns.get_level_values(0) == self._peer_group
        if any(peer_group_index):
            returns = self._gcm_peer_constituent_returns.loc[:, peer_group_index]
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
        return returns

    def _get_peer_rolling_return_summary(self):
        returns = self._primary_peer_constituent_returns.copy()
        rolling_12m_returns = self._helper.get_rolling_return(returns=returns, trailing_months=12)

        rolling_1y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=12)
        rolling_1y_summary = rolling_1y_summary.median(axis=1)

        rolling_3y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=36)
        rolling_3y_summary = rolling_3y_summary.median(axis=1)

        rolling_5y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=60)
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
        rolling_12m_sharpes = self._helper.get_rolling_sharpe_ratio(returns=returns, trailing_months=12,
                                                                    remove_outliers=True)

        rolling_1y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=12)
        rolling_1y_summary = rolling_1y_summary.median(axis=1)

        rolling_3y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=36)
        rolling_3y_summary = rolling_3y_summary.median(axis=1)

        rolling_5y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=60)
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
        trailing_1y_vol = self._helper.get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_vol = self._helper.get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_vol = self._helper.get_trailing_vol(returns=returns, trailing_months=60)
        rolling_1_vol = self._helper.get_rolling_vol(returns=returns, trailing_months=12)
        trailing_5y_median_vol = self._helper.summarize_rolling_median(rolling_1_vol, trailing_months=60)

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
        trailing_1y_beta = self._helper.get_trailing_beta(returns=returns, trailing_months=12)
        trailing_3y_beta = self._helper.get_trailing_beta(returns=returns, trailing_months=36)
        trailing_5y_beta = self._helper.get_trailing_beta(returns=returns, trailing_months=60)
        rolling_1_beta = self._helper.get_rolling_beta(returns=returns, trailing_months=12)
        trailing_5y_median_beta = self._helper.summarize_rolling_median(rolling_1_beta, trailing_months=60)

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
        trailing_1y_sharpe = self._helper.get_trailing_sharpe(returns=returns, trailing_months=12)
        trailing_3y_sharpe = self._helper.get_trailing_sharpe(returns=returns, trailing_months=36)
        trailing_5y_sharpe = self._helper.get_trailing_sharpe(returns=returns, trailing_months=60)
        rolling_1_sharpe = self._helper.get_rolling_sharpe_ratio(returns=returns, trailing_months=12,
                                                                 remove_outliers=True)
        trailing_5y_median_sharpe = self._helper.summarize_rolling_median(rolling_1_sharpe, trailing_months=60)

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
        trailing_1y = self._helper.get_trailing_batting_avg(returns=returns, trailing_months=12)
        trailing_3y = self._helper.get_trailing_batting_avg(returns=returns, trailing_months=36)
        trailing_5y = self._helper.get_trailing_batting_avg(returns=returns, trailing_months=60)
        rolling_1_batting = self._helper.get_rolling_batting_avg(returns=returns, trailing_months=12)
        trailing_5y_median = self._helper.summarize_rolling_median(rolling_1_batting, trailing_months=60)

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
        trailing_1y = self._helper.get_trailing_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_3y = self._helper.get_trailing_win_loss_ratio(returns=returns, trailing_months=36)
        trailing_5y = self._helper.get_trailing_win_loss_ratio(returns=returns, trailing_months=60)
        rolling_1y = self._helper.get_rolling_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_5y_median = self._helper.summarize_rolling_median(rolling_1y, trailing_months=60)

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
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            self._peer_group.replace("/", "") + "_peer_" + as_of_date + ".json",
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
