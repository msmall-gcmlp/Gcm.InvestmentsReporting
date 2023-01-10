import json
from functools import cached_property
import datetime as dt
import pandas as pd
import ast
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from _legacy.Reports.reports.performance_quality.peer_conditional_excess_returns import generate_peer_conditional_excess_returns
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from _legacy.Reports.reports.performance_quality.helper import PerformanceQualityHelper
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import AggregateFromDaily
from gcm.inv.quantlib.enum_source import Periodicity, PeriodicROR


class PerformanceQualityPeerLevelAnalytics(ReportingRunnerBase):
    def __init__(self, peer_group):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._peer_group = peer_group
        self._summary_data_location = "raw/investmentsreporting/summarydata/performancequality"
        self._helper = PerformanceQualityHelper()
        self._analytics = self._helper.analytics

    @cached_property
    def _peer_inputs(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = self._peer_group.replace("/", "") + "_peer_inputs_" + as_of_date + ".json"
        inputs = self._helper.download_inputs(location=self._helper.underlying_data_location, file_path=file)
        return inputs

    @cached_property
    def _peer_returns(self):
        if self._peer_inputs is not None:
            returns = pd.read_json(self._peer_inputs["gcm_peer_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series()

    @cached_property
    def _peer_arb_benchmark_returns(self):
        daily_returns = pd.read_json(self._peer_inputs["peer_group_abs_return_bmrk_returns"], orient="index")
        if daily_returns.columns[0] == 'MOVE Index':
            returns = AggregateFromDaily().transform(
                data=daily_returns,
                method="last",
                period=Periodicity.Monthly,
                first_of_day=True
            )
        else:
            returns = AggregateFromDaily().transform(
                data=daily_returns,
                method="geometric",
                period=Periodicity.Monthly,
                first_of_day=True
            )
        return returns

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
            names=["PeerGroupName", "InvestmentGroupName"],
        )
        returns.columns = returns_columns
        return returns

    @property
    def _constituent_returns(self):
        peer_group_index = self._gcm_peer_constituent_returns.columns.get_level_values(0) == self._peer_group
        if any(peer_group_index):
            returns = self._gcm_peer_constituent_returns.loc[:, peer_group_index]
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
        return returns

    def _get_peer_rolling_return_summary(self):
        returns = self._constituent_returns.copy()
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
        returns = self._constituent_returns.copy()
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
        peer_returns = self._constituent_returns[-120:]

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

    def _calculate_constituent_total_returns(self):
        def _sanitize_list(returns_df):
            return returns_df.dropna().round(4).tolist()

        mtd_returns = self._analytics.compute_periodic_return(
            ror=self._constituent_returns,
            period=PeriodicROR.MTD,
            as_of_date=self._as_of_date,
            method="geometric",
        )

        qtd_returns = self._analytics.compute_periodic_return(
            ror=self._constituent_returns,
            period=PeriodicROR.QTD,
            as_of_date=self._as_of_date,
            method="geometric",
        )
        ytd_returns = self._analytics.compute_periodic_return(
            ror=self._constituent_returns,
            period=PeriodicROR.QTD,
            as_of_date=self._as_of_date,
            method="geometric",
        )

        t1y_returns = self._analytics.compute_trailing_return(
            ror=self._constituent_returns,
            window=12,
            as_of_date=self._as_of_date,
            method="geometric",
            annualize=True,
            periodicity=Periodicity.Monthly,
        )

        t3y_returns = self._analytics.compute_trailing_return(
            ror=self._constituent_returns,
            window=36,
            as_of_date=self._as_of_date,
            method="geometric",
            annualize=True,
            periodicity=Periodicity.Monthly,
        )

        t5y_returns = self._analytics.compute_trailing_return(
            ror=self._constituent_returns,
            window=60,
            as_of_date=self._as_of_date,
            method="geometric",
            annualize=True,
            periodicity=Periodicity.Monthly,
        )

        t10y_returns = self._analytics.compute_trailing_return(
            ror=self._constituent_returns,
            window=120,
            as_of_date=self._as_of_date,
            method="geometric",
            annualize=True,
            periodicity=Periodicity.Monthly,
        )

        periodic_returns = {PeriodicROR.MTD.value: _sanitize_list(mtd_returns),
                            PeriodicROR.QTD.value: _sanitize_list(qtd_returns),
                            PeriodicROR.YTD.value: _sanitize_list(ytd_returns),
                            'T12': _sanitize_list(t1y_returns),
                            'T36': _sanitize_list(t3y_returns),
                            'T60': _sanitize_list(t5y_returns),
                            'T120': _sanitize_list(t10y_returns)}
        return periodic_returns

    def _summarize_peer_counts(self):
        counts = self._helper.summarize_counts(returns=self._constituent_returns)
        return {'counts': [int(x) for x in counts]}

    def generate_peer_level_summaries(self):
        constituent_total_returns = self._calculate_constituent_total_returns()
        market_scenarios, conditional_ptile_summary = \
            generate_peer_conditional_excess_returns(peer_returns=self._constituent_returns,
                                                     benchmark_returns=self._peer_arb_benchmark_returns)

        condl_mkt_bmrk = market_scenarios.columns[0]
        condl_mkt_bmrk = pd.DataFrame({"condl_mkt_bmrk": [condl_mkt_bmrk]})

        condl_peer_heading = self._peer_group.replace("GCM", "") + " Peer Percentile"
        condl_peer_heading = pd.DataFrame({"condl_peer_heading": [condl_peer_heading]})

        # hack to display as non-percentage in Excel
        condl_mkt_return = conditional_ptile_summary.iloc[:, 0]
        if condl_mkt_return.name == 'MOVE Index':
            condl_mkt_return = condl_mkt_return.astype(int).astype(str) + ' '
            condl_mkt_return[0] = condl_mkt_return[0] + '(Lvl)'

        performance_stability_summary = self.build_performance_stability_peer_summary()

        peer_counts = self._summarize_peer_counts()

        input_data_json = {
            "performance_stability_peer_summary": performance_stability_summary.to_json(orient="index"),
            "condl_mkt_bmrk": condl_mkt_bmrk.to_json(orient="index"),
            "condl_mkt_return": condl_mkt_return.to_json(orient="index"),
            "condl_peer_excess_returns": conditional_ptile_summary.iloc[:, 1:].to_json(orient="index"),
            "condl_peer_heading": condl_peer_heading.to_json(orient="index"),
            "market_scenarios_3y": market_scenarios.to_json(orient="index"),
            "market_returns_monthly": self._peer_arb_benchmark_returns.to_json(orient="index"),
            "constituent_total_returns": constituent_total_returns,
            "gcm_peer_returns": self._gcm_peer_returns.to_json(orient="index"),
            "peer_counts": peer_counts
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
        self.generate_peer_level_summaries()
        return self._peer_group + " Complete"


if __name__ == "__main__":
    runner = DaoRunner()
    # runner = DaoRunner(
    #     container_lambda=lambda b, i: b.config.from_dict(i),
    #     config_params={
    #         DaoRunnerConfigArgs.dao_global_envs.name: {
    #             DaoSource.InvestmentsDwh.name: {
    #                 "Environment": "prd",
    #                 "Subscription": "prd",
    #             },
    #             DaoSource.PubDwh.name: {
    #                 "Environment": "prd",
    #                 "Subscription": "prd",
    #             },
    #         }
    #     },
    # )

    with Scenario(runner=runner, as_of_date=dt.date(2022, 10, 31)).context():
        analytics = PerformanceQualityPeerLevelAnalytics(peer_group='GCM Multi-PM').execute()
