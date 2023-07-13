import json
import logging
import pandas as pd
import ast
import numpy as np
import datetime as dt
from functools import partial, cached_property

import scipy
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from _legacy.Reports.reports.performance_quality.helper import PerformanceQualityHelper
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.inv.quantlib.enum_source import PeriodicROR, Periodicity
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario
from gcm.inv.models.ars_peer_analysis.peer_conditional_excess_returns import _compute_rolling_excess_metrics
from gcm.inv.models.ars_peer_analysis.peer_conditional_excess_returns import generate_peer_conditional_excess_returns
from _legacy.Reports.reports.performance_quality.peer_level_analytics import PerformanceQualityPeerLevelAnalytics


class PerformanceQualityReport(ReportingRunnerBase):

    def __init__(self, fund_name):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._fund_name = fund_name
        self._summary_data_location = "raw/investmentsreporting/summarydata/performancequality"
        self._helper = PerformanceQualityHelper()
        self._analytics = self._helper.analytics

    @cached_property
    def _fund_inputs(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = self._fund_name.replace("/", "") + "_fund_inputs_" + as_of_date + ".json"
        inputs = self._helper.download_inputs(location=self._helper.underlying_data_location, file_path=file)
        return inputs

    @cached_property
    def _peer_inputs(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = self._primary_peer_group.replace("/", "") + "_peer_inputs_" + as_of_date + ".json"
        inputs = self._helper.download_inputs(location=self._helper.underlying_data_location, file_path=file)
        return inputs

    @cached_property
    def _fund_dimn(self):
        fund_dimn = pd.read_json(self._fund_inputs["fund_dimn"], orient="index")
        return fund_dimn

    @cached_property
    def _entity_type(self):
        return "ARS PFUND"

    @cached_property
    def _fund_id(self):
        return self._fund_dimn["InvestmentGroupId"]

    @cached_property
    def _pub_investment_group_id(self):
        return self._fund_dimn["PubInvestmentGroupId"].squeeze()

    @cached_property
    def _substrategy(self):
        return self._fund_dimn["SubStrategy"].squeeze()

    @cached_property
    def _primary_peer_group(self):
        group = self._fund_dimn["ReportingPeerGroup"].squeeze()
        if not isinstance(group, str):
            group = None
        return group

    @cached_property
    def _secondary_peer_group(self):
        group = self._fund_dimn["StrategyPeerGroup"].squeeze()
        if not isinstance(group, str):
            group = None
        return group

    @cached_property
    def _eurekahedge_benchmark(self):
        group = self._fund_dimn["EurekahedgeBenchmark"].squeeze()
        if not isinstance(group, str):
            group = None
        return group

    @cached_property
    def _abs_return_benchmark(self):
        return self._fund_dimn["AbsoluteBenchmarkName"].squeeze()

    @cached_property
    def _fle_scl(self):
        return self._fund_dimn["FleScl"].squeeze().round(2)

    @cached_property
    def _fund_expectations(self):
        expectations = pd.read_json(self._fund_inputs["expectations"], orient="index")
        return expectations

    @cached_property
    def _fund_fl_expected_returns(self):
        expected_return = pd.read_json(self._fund_inputs["fl_expected_returns"], orient="index")
        return expected_return

    @cached_property
    def _fund_distributions(self):
        distributions = pd.read_json(self._fund_inputs["distributions"], orient="index")
        return distributions

    @cached_property
    def _risk_model_expected_return(self):
        if self._fund_fl_expected_returns.shape[0] == 0:
            return np.nan
        else:
            return self._fund_fl_expected_returns["ExpectedReturn"].squeeze().round(2)

    @cached_property
    def _risk_model_expected_vol(self):
        if self._fund_expectations.shape[0] == 0:
            return np.nan
        else:
            return self._fund_expectations["Volatility"].squeeze().round(2)

    @cached_property
    def _fund_returns(self):
        fund_returns = pd.read_json(self._fund_inputs["fund_returns"], orient="index")
        fund_returns = fund_returns.sort_index()

        if any(fund_returns.columns == self._fund_name):
            return fund_returns[self._fund_name].to_frame()
        else:
            return pd.DataFrame()

    @cached_property
    def _fund_gross_exposures(self):
        fund_gross_exposures = pd.read_json(self._fund_inputs["gross_exposures"], orient="index")
        fund_gross_exposures = fund_gross_exposures.sort_index()

        if any(fund_gross_exposures.columns == self._fund_name):
            return fund_gross_exposures[self._fund_name].to_frame()
        else:
            return pd.DataFrame()

    @cached_property
    def _fund_net_exposures(self):
        fund_net_exposures = pd.read_json(self._fund_inputs["net_exposures"], orient="index")
        fund_net_exposures = fund_net_exposures.sort_index()

        if any(fund_net_exposures.columns == self._fund_name):
            return fund_net_exposures[self._fund_name].to_frame()
        else:
            return pd.DataFrame()

    @cached_property
    def _itd_months(self):
        all_returns = self._fund_returns.merge(self._primary_peer_returns, left_index=True, right_index=True)
        all_returns = all_returns.merge(self._ehi50_returns, left_index=True, right_index=True)
        all_returns = all_returns.merge(self._ehi200_returns, left_index=True, right_index=True)
        all_returns = all_returns.merge(self._abs_bmrk_returns, left_index=True, right_index=True)
        return all_returns.shape[0]

    @cached_property
    def _abs_bmrk_returns(self):
        returns = pd.read_json(self._fund_inputs["abs_bmrk_returns"], orient="index")
        if len(returns) <= 1:
            returns = pd.DataFrame()

        if any(self._fund_id.squeeze() == list(returns.columns)):
            returns = returns[self._fund_id].squeeze()
        else:
            returns = pd.DataFrame()
        return returns

    @cached_property
    def _abs_bmrk_betas(self):
        return pd.read_json(self._fund_inputs["abs_bmrk_betas"], orient="index")

    @cached_property
    def _exposure(self):
        latest = pd.read_json(self._fund_inputs["exposure_latest"], orient="index")
        latest["Period"] = "Latest"
        three = pd.read_json(self._fund_inputs["exposure_3y"], orient="index")
        three["Period"] = "3Y"
        five = pd.read_json(self._fund_inputs["exposure_5y"], orient="index")
        five["Period"] = "5Y"
        ten = pd.read_json(self._fund_inputs["exposure_10y"], orient="index")
        ten["Period"] = "10Y"
        all_exposure = pd.concat([latest, three, five, ten])

        if all_exposure.shape[0] > 0:
            exposure = all_exposure[all_exposure["InvestmentGroupName"] == self._fund_name]
            exposure = exposure.set_index("Period")
            return exposure
        else:
            return pd.DataFrame(
                columns=[
                    "InvestmentGroupName",
                    "InvestmentGroupId",
                    "Date",
                    "LongNotional",
                    "ShortNotional",
                    "GrossNotional",
                    "NetNotional",
                ],
                index=["Latest", "3Y", "5Y", "10Y"],
            )

    @cached_property
    def _primary_peer_returns(self):
        if self._primary_peer_analytics is not None:
            # returns = pd.read_json(self._primary_peer_analytics["gcm_peer_returns"], orient="index")
            returns = pd.read_json(self._peer_inputs["gcm_peer_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series(name='PrimaryPeer')

    @cached_property
    def _primary_peer_constituent_returns(self):
        # pd.read_json(self._peer_inputs["gcm_peer_constituent_returns"], orient="index")
        if self._peer_inputs is not None:
            returns = pd.read_json(self._peer_inputs["gcm_peer_constituent_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series(name='PrimaryPeer')

    @cached_property
    def _primary_peer_counts(self):
        if self._primary_peer_analytics is not None:
            return self._primary_peer_analytics["peer_counts"].get('counts')
        else:
            return None

    @cached_property
    def _secondary_peer_counts(self):
        if self._secondary_peer_analytics is not None:
            return self._secondary_peer_analytics["peer_counts"].get('counts')
        else:
            return None

    @cached_property
    def _eurekahedge_inputs(self):
        if self._eurekahedge_benchmark is not None:
            as_of_date = self._as_of_date.strftime("%Y-%m-%d")
            file = self._eurekahedge_benchmark.replace("/", "") + "_eurekahedge_inputs_" + as_of_date + ".json"
            eurekahedge_inputs = self._helper.download_inputs(
                location=self._helper.underlying_data_location, file_path=file
            )
        else:
            eurekahedge_inputs = None
        return eurekahedge_inputs

    @cached_property
    def _eurekahedge200_inputs(self):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = "Eurekahedge Institutional 200" + "_eurekahedge_inputs_" + as_of_date + ".json"
        eurekahedge200_inputs = self._helper.download_inputs(
            location=self._helper.underlying_data_location, file_path=file
        )
        return eurekahedge200_inputs

    @cached_property
    def _ehi50_returns(self):
        inputs = self._eurekahedge_inputs
        if inputs is not None:
            returns = pd.read_json(inputs["eurekahedge_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series(name='Ehi50')

    @cached_property
    def _ehi200_returns(self):
        returns = pd.read_json(
            self._eurekahedge200_inputs["eurekahedge_returns"],
            orient="index",
        )
        if isinstance(returns, pd.DataFrame):
            return returns.squeeze()
        else:
            return pd.Series(name='Ehi200')

    @cached_property
    def _eurekahedge_constituent_returns(self):
        inputs = self._eurekahedge_inputs
        if inputs is not None:
            returns = pd.read_json(inputs["eurekahedge_constituent_returns"], orient="index")
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(
                returns_columns,
                names=["EurekahedgeBenchmark", "SourceInvestmentId"],
            )
            returns.columns = returns_columns
            return returns.droplevel(0, axis=1)
        else:
            return pd.Series()

    @cached_property
    def _ehi200_constituent_returns(self):
        returns = pd.read_json(
            self._eurekahedge200_inputs["eurekahedge_constituent_returns"],
            orient="index",
        )
        returns_columns = [ast.literal_eval(x) for x in returns.columns]
        returns_columns = pd.MultiIndex.from_tuples(
            returns_columns,
            names=["EurekahedgeBenchmark", "SourceInvestmentId"],
        )
        returns.columns = returns_columns

        if isinstance(returns, pd.DataFrame):
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.Series()

        return returns

    @cached_property
    def _fund_rba(self):
        rba = pd.read_json(self._fund_inputs["rba"], orient="index")
        rba_columns = [ast.literal_eval(x) for x in rba.columns]
        rba_columns = pd.MultiIndex.from_tuples(rba_columns, names=["FactorGroup1", "AggLevel"])
        rba.columns = rba_columns

        if rba is not None:
            return rba
        else:
            return pd.DataFrame()

    @cached_property
    def _fund_rba_risk_decomp(self):
        decomp = pd.read_json(self._fund_inputs["rba_risk_decomp"], orient="index")
        decomp = decomp[decomp["InvestmentGroupName"] == self._fund_name]
        decomp = decomp[["FactorGroup1", "1Y", "3Y", "5Y"]]
        decomp.rename(columns={"1Y": "TTM"}, inplace=True)
        mapping = pd.DataFrame(
            {
                "FactorGroup1": [
                    "SYSTEMATIC",
                    "X_ASSET_CLASS",
                    "PUBLIC_LS",
                    "NON_FACTOR",
                ],
                "Group": [
                    "SYSTEMATIC_RISK",
                    "X_ASSET_RISK",
                    "PUBLIC_LS_RISK",
                    "NON_FACTOR_RISK",
                ],
            }
        )

        decomp = decomp.merge(mapping, how="left").groupby("Group").sum()

        risk_decomp_columns = pd.DataFrame(
            columns=[
                "SYSTEMATIC_RISK",
                "X_ASSET_RISK",
                "PUBLIC_LS_RISK",
                "NON_FACTOR_RISK",
            ]
        )
        risk_decomp = pd.concat([risk_decomp_columns, decomp.T])
        risk_decomp = risk_decomp.fillna(0)
        risk_decomp = risk_decomp.round(2)
        return risk_decomp

    @cached_property
    def _fund_rba_adj_r_squared(self):
        r2 = pd.read_json(self._fund_inputs["rba_adj_r_squared"], orient="index")

        if r2.shape[0] == 0:
            return pd.DataFrame(index=["TTM", "3Y", "5Y"], columns=["AdjR2"])

        r2 = r2[r2["InvestmentGroupName"] == self._fund_name]
        r2 = r2[["1Y", "3Y", "5Y"]]
        r2.rename(columns={"1Y": "TTM"}, inplace=True)

        r2 = r2.T
        r2.columns = ["AdjR2"]
        r2 = r2.round(2)
        return r2

    @cached_property
    def _fund_pba_publics(self):
        pba = pd.read_json(self._fund_inputs["pba_publics"], orient="index")
        pba_columns = [ast.literal_eval(x) for x in pba.columns]
        pba_columns = pd.MultiIndex.from_tuples(pba_columns, names=["FactorGroup1", "InvestmentGroupId"])
        pba.columns = pba_columns

        fund_index = pba.columns.get_level_values(1) == self._fund_id.squeeze()
        if any(fund_index):
            fund_pba = pba.iloc[:, fund_index]
            fund_pba.columns = fund_pba.columns.droplevel(1)
            return fund_pba
        else:
            return pd.DataFrame()

    @cached_property
    def _fund_pba_privates(self):
        pba = pd.read_json(self._fund_inputs["pba_privates"], orient="index")
        pba_columns = [ast.literal_eval(x) for x in pba.columns]
        pba_columns = pd.MultiIndex.from_tuples(pba_columns, names=["FactorGroup1", "InvestmentGroupId"])
        pba.columns = pba_columns

        fund_index = pba.columns.get_level_values(1) == self._fund_id.squeeze()
        if any(fund_index):
            fund_pba = pba.iloc[:, fund_index]
            fund_pba.columns = fund_pba.columns.droplevel(1)
            return fund_pba
        else:
            return pd.DataFrame()

    @cached_property
    def _latest_exposure_date(self):
        latest = self._exposure.loc["Latest"]
        if isinstance(latest, pd.Series):
            date = latest.loc["Date"]
        else:
            date = self._exposure.loc["Latest"][0:1]["Date"].squeeze()

        if isinstance(date, dt.datetime):
            return date
        else:
            return None

    def get_header_info(self):
        if isinstance(self._substrategy, str):
            entity_type = self._entity_type + " - " + self._substrategy
        else:
            entity_type = self._entity_type

        header = pd.DataFrame(
            {
                "header_info": [
                    self._fund_name,
                    entity_type,
                    self._as_of_date,
                ]
            }
        )
        return header

    def _download_peer_analytics(self, peer_group):
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        file = peer_group.replace("/", "") + "_peer_" + as_of_date + ".json"
        return self._helper.download_inputs(location=self._summary_data_location, file_path=file)

    @cached_property
    def _primary_peer_analytics(self):
        if self._primary_peer_group is None:
            return None
        return self._download_peer_analytics(peer_group=self._primary_peer_group)

    @cached_property
    def _secondary_peer_analytics(self):
        if self._secondary_peer_group is None:
            return None
        return self._download_peer_analytics(peer_group=self._secondary_peer_group)

    @cached_property
    def _condl_mkt_bmrk(self):
        if self._primary_peer_group is not None:
            bmrk = pd.read_json(self._primary_peer_analytics["condl_mkt_bmrk"], orient="index")
        else:
            bmrk = pd.DataFrame()
        return bmrk

    @cached_property
    def _condl_mkt_return(self):
        if self._primary_peer_group is not None:
            returns = pd.read_json(self._primary_peer_analytics["condl_mkt_return"], orient="index")
        else:
            returns = pd.DataFrame(index=['< 10th', '10th - 25th', '25th - 50th',
                                          '50th - 75th', '75th - 90th', '> 90th'],
                                   columns=[0])
        return returns

    @cached_property
    def _condl_peer_excess_returns(self):
        if self._primary_peer_group is not None:
            returns = pd.read_json(self._primary_peer_analytics["condl_peer_excess_returns"], orient="index")
        else:
            returns = pd.DataFrame(index=['< 10th', '10th - 25th', '25th - 50th',
                                          '50th - 75th', '75th - 90th', '> 90th'],
                                   columns=[10, 25, 50, 75, 90])
        return returns

    @cached_property
    def _condl_peer_heading(self):
        if self._primary_peer_group is not None:
            heading = pd.read_json(self._primary_peer_analytics["condl_peer_heading"], orient="index")
        else:
            heading = pd.DataFrame()
        return heading

    @cached_property
    def _market_scenarios_3y(self):
        if self._primary_peer_group is not None:
            scenarios = pd.read_json(self._primary_peer_analytics["market_scenarios_3y"], orient="index")
        else:
            scenarios = None
        return scenarios

    @cached_property
    def _market_returns_monthly(self):
        if self._primary_peer_group is not None:
            scenarios = pd.read_json(self._primary_peer_analytics["market_returns_monthly"], orient="index")
        else:
            scenarios = None
        return scenarios

    @cached_property
    def _primary_peer_total_returns(self):
        if self._primary_peer_group is not None:
            return self._primary_peer_analytics["constituent_total_returns"]
        else:
            return None

    # @cached_property
    # def _primary_peer_excess_returns(self):
    #     if self._primary_peer_group is not None:
    #         return self._primary_peer_analytics["constituent_excess_returns"]
    #     else:
    #         return None

    @cached_property
    def _secondary_peer_total_returns(self):
        if self._secondary_peer_group is not None:
            return self._secondary_peer_analytics["constituent_total_returns"]
        else:
            return None

    def get_peer_group_heading(self):
        if self._primary_peer_group is not None:
            group = self._primary_peer_group + " Peer"
            return pd.DataFrame({"peer_group_heading": ["v. " + group]})
        else:
            return pd.DataFrame({"peer_group_heading": ["v. GCM Peer"]})

    def get_absolute_return_benchmark(self):
        if self._abs_return_benchmark is not None:
            return pd.DataFrame({"absolute_return_benchmark": [self._abs_return_benchmark]})
        else:
            return pd.DataFrame({"absolute_return_benchmark": ["To be agreed"]})

    def get_eurekahedge_benchmark_heading(self):
        if self._eurekahedge_benchmark is not None:
            return pd.DataFrame({"eurekahedge_benchmark_heading": ["v. " + self._eurekahedge_benchmark]})
        else:
            return pd.DataFrame({"eurekahedge_benchmark_heading": ["v. EHI Index"]})

    def get_peer_ptile_1_heading(self):
        if self._primary_peer_group is not None:
            group = self._primary_peer_group.replace("GCM ", "")
            return pd.DataFrame({"peer_ptile_1_heading": [group]})
        else:
            return pd.DataFrame({"peer_ptile_1_heading": [""]})

    # def get_peer_ptile_2_heading(self):
    #     if self._secondary_peer_group is not None:
    #         group = self._secondary_peer_group.replace("GCM ", "")
    #         return pd.DataFrame({"peer_ptile_2_heading": [group]})
    #     else:
    #         return pd.DataFrame({"peer_ptile_2_heading": [""]})

    def get_latest_exposure_heading(self):
        if self._latest_exposure_date is not None:
            heading = "Latest (" + self._latest_exposure_date.strftime("%b %Y") + ")"
            return pd.DataFrame({"latest_exposure_heading": [heading]})
        else:
            return pd.DataFrame({"latest_exposure_heading": [""]})

    def _get_return_summary(self, returns, return_type):
        returns = returns.copy()
        if returns.shape[0] == 0:
            return pd.DataFrame(
                index=["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y", "ITD"],
                columns=["Fund"],
            )

        mtd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.MTD,
            as_of_date=self._as_of_date,
            method="geometric",
        )

        qtd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.QTD,
            as_of_date=self._as_of_date,
            method="geometric",
        )

        ytd_return = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.YTD,
            as_of_date=self._as_of_date,
            method="geometric",
        )

        trailing_1y_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=12,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )

        trailing_3y_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=36,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )

        trailing_5y_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=60,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )

        trailing_10y_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=120,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )

        itd_return = self._analytics.compute_trailing_return(
            ror=returns,
            window=self._itd_months,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True if self._itd_months > 12 else False,
        )

        # rounding to 2 so that Excess Return matches optically
        stats = [
            mtd_return,
            qtd_return,
            ytd_return,
            trailing_1y_return,
            trailing_3y_return,
            trailing_5y_return,
            trailing_10y_return,
            itd_return
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {return_type: [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y", "ITD"],
        )
        return summary

    def _get_excess_return_summary(self, fund_returns, benchmark_returns, benchmark_name):
        fund_returns = fund_returns.copy().fillna(" ")
        benchmark_returns = benchmark_returns.copy()
        if benchmark_returns.shape[0] > 0:
            benchmark_returns = self._get_return_summary(returns=benchmark_returns, return_type=benchmark_name)
            summary = fund_returns.merge(benchmark_returns, left_index=True, right_index=True)
            summary["IsNumeric"] = summary.applymap(np.isreal).all(1)
            excess = summary[summary["IsNumeric"]]["Fund"] - summary[summary["IsNumeric"]][benchmark_name]
            summary[benchmark_name + "Excess"] = excess.astype(float).round(2)
            summary.drop(columns={"IsNumeric"}, inplace=True)
            summary = summary.fillna("")
        else:
            summary = fund_returns.copy()
            summary[benchmark_name] = ""
            summary[benchmark_name + "Excess"] = ""

        return summary

    def _calculate_periodic_percentile(self, fund_periodic_return, period, peer_monthly_rors=None,
                                       peer_total_returns=None):
        if peer_total_returns is None:
            periodic = self._analytics.compute_periodic_return(
                ror=peer_monthly_rors,
                period=period,
                as_of_date=self._as_of_date,
                method="geometric",
            )
        else:
            periodic = pd.Series(peer_total_returns.get(period.value))
        periodics = pd.concat([pd.Series([fund_periodic_return.squeeze()]), periodic], axis=0)
        ptile = periodics.rank(pct=True)[0:1].squeeze().round(2) * 100
        return ptile

    def _calculate_trailing_percentile(self, fund_periodic_return, trailing_months, peer_monthly_rors=None,
                                       peer_total_returns=None):
        if peer_total_returns is None:
            returns = self._analytics.compute_trailing_return(
                ror=peer_monthly_rors,
                window=trailing_months,
                as_of_date=self._as_of_date,
                method="geometric",
                annualize=True,
                periodicity=Periodicity.Monthly,
            )
        else:
            returns = pd.Series(peer_total_returns.get('T' + str(trailing_months)))

        if isinstance(fund_periodic_return.squeeze(), float):
            returns = pd.concat(
                [pd.Series([fund_periodic_return.squeeze()]), returns],
                axis=0,
            )
            ptile = returns.rank(pct=True)[0:1].squeeze().round(2) * 100
        else:
            ptile = ""
        return ptile

    def _get_percentile_summary(self, fund_returns, group_name, constituent_monthly_rors=None,
                                constituent_total_returns=None):
        fund_returns = fund_returns.copy()
        index = ["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y"]

        if constituent_monthly_rors is None or len(constituent_monthly_rors) == 0:
            if constituent_total_returns is None or len(constituent_total_returns) == 0:
                return pd.DataFrame({group_name: [""] * len(index)}, index=index)

        if fund_returns.loc["MTD"].squeeze() == ' ':
            return pd.DataFrame({group_name: [""] * len(index)}, index=index)

        mtd_ptile = self._calculate_periodic_percentile(fund_periodic_return=fund_returns.loc["MTD"],
                                                        period=PeriodicROR.MTD,
                                                        peer_monthly_rors=constituent_monthly_rors,
                                                        peer_total_returns=constituent_total_returns)
        qtd_ptile = self._calculate_periodic_percentile(fund_periodic_return=fund_returns.loc["QTD"],
                                                        period=PeriodicROR.QTD,
                                                        peer_monthly_rors=constituent_monthly_rors,
                                                        peer_total_returns=constituent_total_returns)
        ytd_ptile = self._calculate_periodic_percentile(fund_periodic_return=fund_returns.loc["YTD"],
                                                        period=PeriodicROR.YTD,
                                                        peer_monthly_rors=constituent_monthly_rors,
                                                        peer_total_returns=constituent_total_returns)
        trailing_1y_ptile = self._calculate_trailing_percentile(fund_periodic_return=fund_returns.loc["TTM"],
                                                                trailing_months=12,
                                                                peer_monthly_rors=constituent_monthly_rors,
                                                                peer_total_returns=constituent_total_returns)
        trailing_3y_ptile = self._calculate_trailing_percentile(fund_periodic_return=fund_returns.loc["3Y"],
                                                                trailing_months=36,
                                                                peer_monthly_rors=constituent_monthly_rors,
                                                                peer_total_returns=constituent_total_returns)
        trailing_5y_ptile = self._calculate_trailing_percentile(fund_periodic_return=fund_returns.loc["5Y"],
                                                                trailing_months=60,
                                                                peer_monthly_rors=constituent_monthly_rors,
                                                                peer_total_returns=constituent_total_returns)
        trailing_10y_ptile = self._calculate_trailing_percentile(fund_periodic_return=fund_returns.loc["10Y"],
                                                                 trailing_months=120,
                                                                 peer_monthly_rors=constituent_monthly_rors,
                                                                 peer_total_returns=constituent_total_returns)

        summary = pd.DataFrame(
            {
                group_name: [
                    mtd_ptile,
                    qtd_ptile,
                    ytd_ptile,
                    trailing_1y_ptile,
                    trailing_3y_ptile,
                    trailing_5y_ptile,
                    trailing_10y_ptile,
                ]
            },
            index=index,
        )

        return summary

    def peer_3y_data(self, excess_peer_data):
        ret_3y = pd.DataFrame()
        for col in excess_peer_data.columns:
            # monthly_ret_total_data=total_peer_data[col]
            monthly_ret_excess_data = excess_peer_data[col]
            ret_3y[col] = self._analytics.compute_trailing_return(
                ror=monthly_ret_excess_data,
                window=36,
                as_of_date=self._as_of_date,
                method="geometric",
                periodicity=Periodicity.Monthly,
                annualize=True,
            )
        return ret_3y

    def build_ar_peer_excess(self):
        peer_arb_bmrk = PerformanceQualityPeerLevelAnalytics(peer_group=self._primary_peer_group)._peer_arb_benchmark_returns
        peer_rors = PerformanceQualityPeerLevelAnalytics(peer_group=self._primary_peer_group)._constituent_returns

        rf_ret = self._helper.rf_return
        rf_df = pd.DataFrame(rf_ret)
        peer_bmrk_ror = peer_rors.merge(peer_arb_bmrk, how='left', left_index=True, right_index=True)
        passive_bmrk = peer_arb_bmrk.columns[0]
        betas = []

        bmrk_df = pd.DataFrame(peer_bmrk_ror[passive_bmrk])
        bmrk_and_rf = bmrk_df.merge(rf_df, how='left', left_index=True, right_index=True)
        bmrk_and_rf['1M_RiskFree'] = bmrk_and_rf['1M_RiskFree'].fillna(0)
        risk_free = bmrk_and_rf['1M_RiskFree']

        peer_beta_adj_index = pd.DataFrame()
        if passive_bmrk != 'MOVE Index':
            for fund in peer_bmrk_ror.columns[:-1]:
                fund_returns = peer_bmrk_ror[[passive_bmrk, fund]].dropna()

                if fund_returns.shape[0] > 0:
                    beta = scipy.stats.linregress(x=fund_returns[passive_bmrk], y=fund_returns[fund])[0]
                    beta = round(max(min(beta, 1.5), 0.1), 1)
                    betas.append(beta)

                    beta_adj_index = peer_bmrk_ror[passive_bmrk] * beta + (1 - beta) * risk_free
                    peer_beta_adj_index[fund] = beta_adj_index
        else:
            for fund in peer_bmrk_ror.columns[:-1]:
                peer_beta_adj_index[fund] = risk_free

        peer_xs_ret_summary = pd.DataFrame()
        peer_rors = peer_rors.dropna(axis=0, how='all')
        for col in peer_rors.columns:
            peer_fund_summary = self._get_return_summary(returns=peer_rors[col], return_type="Fund")
            peer_fund_xs = self._get_excess_return_summary(
                fund_returns=peer_fund_summary,
                benchmark_returns=peer_beta_adj_index[col],
                benchmark_name="AbsoluteReturnBenchmark",
            )
            peer_fund_xs['AbsoluteReturnBenchmarkExcess'].replace('', np.nan, inplace=True)
            curr_peer_xs = pd.DataFrame(peer_fund_xs["AbsoluteReturnBenchmarkExcess"])
            curr_peer_xs = curr_peer_xs.rename(columns={'AbsoluteReturnBenchmarkExcess': col})
            curr_peer_xs = curr_peer_xs.transpose()
            peer_xs_ret_summary = pd.concat([peer_xs_ret_summary, curr_peer_xs])
        peer_xs_ret_summary.dropna(how='all', inplace=True)
        peer_excess_df = peer_xs_ret_summary.iloc[:, :-1]

        peer_excess_df.rename(columns={'MTD': 'M', 'QTD': 'Q', 'YTD': 'Y', 'TTM': 'T12', '3Y': 'T36', '5Y': 'T60', '10Y': 'T120'
                                       }, inplace=True)
        peer_excess_df = {peer_excess_df[column].name: [y for y in peer_excess_df[column] if not pd.isna(y)] for column in peer_excess_df}
        return peer_excess_df

    def build_benchmark_summary(self):
        fund_returns = self._get_return_summary(returns=self._fund_returns, return_type="Fund")
        absolute_return_summary = self._get_excess_return_summary(
            fund_returns=fund_returns,
            benchmark_returns=self._abs_bmrk_returns,
            benchmark_name="AbsoluteReturnBenchmark",
        )
        gcm_peer_summary = self._get_excess_return_summary(
            fund_returns=fund_returns,
            benchmark_returns=self._primary_peer_returns,
            benchmark_name="GcmPeer",
        )
        ehi_50_summary = self._get_excess_return_summary(
            fund_returns=fund_returns,
            benchmark_returns=self._ehi50_returns,
            benchmark_name="EHI50",
        )
        ehi_200_summary = self._get_excess_return_summary(
            fund_returns=fund_returns,
            benchmark_returns=self._ehi200_returns,
            benchmark_name="EHI200",
        )
        fund_excess_returns = pd.DataFrame(absolute_return_summary.iloc[:, -1])
        peer_excess_dict = self.build_ar_peer_excess()

        primary_peer_percentiles = self._get_percentile_summary(fund_returns=fund_returns,
                                                                group_name="Peer1Ptile",
                                                                constituent_total_returns=self._primary_peer_total_returns)

        primary_peer_excess_arb_percentiles = self._get_percentile_summary(
                                                                fund_returns=fund_excess_returns,
                                                                group_name="Peer1Ptile",
                                                                constituent_total_returns=peer_excess_dict)

        eurekahedge_percentiles = self._get_percentile_summary(fund_returns=fund_returns,
                                                               group_name="EH50Ptile",
                                                               constituent_monthly_rors=self._eurekahedge_constituent_returns)

        ehi200_percentiles = self._get_percentile_summary(fund_returns=fund_returns,
                                                          group_name="EHI200Ptile",
                                                          constituent_monthly_rors=self._ehi200_constituent_returns)

        summary = absolute_return_summary.copy()
        summary = summary.merge(
            gcm_peer_summary.drop(columns={"Fund"}),
            left_index=True,
            right_index=True,
        )
        summary = summary.merge(
            ehi_50_summary.drop(columns={"Fund"}),
            left_index=True,
            right_index=True,
        )
        summary = summary.merge(
            ehi_200_summary.drop(columns={"Fund"}),
            left_index=True,
            right_index=True,
        )
        summary = summary.merge(primary_peer_percentiles,
                                left_index=True, right_index=True,
                                how='left')
        summary = summary.merge(primary_peer_excess_arb_percentiles,
                                left_index=True, right_index=True,
                                how='left')
        summary = summary.merge(eurekahedge_percentiles,
                                left_index=True,
                                right_index=True,
                                how='left')
        summary = summary.merge(ehi200_percentiles,
                                left_index=True,
                                right_index=True,
                                how='left')

        summary = summary.fillna("")
        return summary

    def build_constituent_count_summary(self):
        eureka = self._helper.summarize_counts(
            returns=self._eurekahedge_constituent_returns)
        ehi200 = self._helper.summarize_counts(
            returns=self._ehi200_constituent_returns)

        summary = pd.DataFrame({"primary": self._primary_peer_counts,
                                "secondary": self._primary_peer_counts,
                                "eureka": eureka,
                                "ehi200": ehi200})
        return summary

    @staticmethod
    def _get_exposure_summary(exposure):
        strategies = ["Equities", "Credit", "Macro"]
        exposures = [x + "Notional" for x in ["Long", "Short", "Gross", "Net"]]
        column_index = pd.MultiIndex.from_product(
            [strategies, exposures], names=["ExposureStrategy", "ExposureType"])
        periods = ["Latest", "3Y", "5Y", "10Y"]

        exposure_summary = exposure.drop(
            columns={"InvestmentGroupName", "InvestmentGroupId", "Date"})

        if all(exposure_summary.isna().all()):
            return pd.DataFrame(index=periods, columns=column_index)

        macro_strat = ~exposure_summary[
            "ExposureStrategy"].isin(["Equities", "Credit"])
        exposure_summary.loc[macro_strat, "ExposureStrategy"] = "Macro"
        exposure_summary = exposure_summary.groupby(
            ["Period", "ExposureStrategy"]).sum()
        exposure_summary = exposure_summary.reset_index().set_index("Period")
        exposure_summary = exposure_summary.pivot(columns=["ExposureStrategy"])
        exposure_summary.columns = exposure_summary.columns.reorder_levels([1, 0])

        exposure_summary = exposure_summary.reindex(column_index, axis=1)
        exposure_summary = exposure_summary.reindex(periods)
        summary = exposure_summary.loc[periods]
        summary = summary.round(2)
        summary = summary * 100
        return summary

    def _get_rba_summary(self):
        factor_group_index = pd.DataFrame(
            index=[
                "SYSTEMATIC",
                "REGION",
                "INDUSTRY",
                "X_ASSET_CLASS_EXCLUDED",
                "LS_EQUITY",
                "LS_CREDIT",
                "MACRO",
                "NON_FACTOR_SECURITY_SELECTION",
                "NON_FACTOR_OUTLIER_EFFECTS",
            ]
        )
        fund_rba = self._fund_rba.copy()
        fund_rba.columns = fund_rba.columns.droplevel(0)

        quarter_start = dt.datetime(
            self._as_of_date.year,
            3 * ((self._as_of_date.month - 1) // 3) + 1,
            1,
        ).month
        qtd_window = self._as_of_date.month - quarter_start + 1
        ytd_window = self._as_of_date.month

        p_return_attribution = partial(
            self._analytics.compute_return_attributions,
            attribution_ts=fund_rba,
            periodicity=Periodicity.Monthly,
            as_of_date=self._as_of_date,
        )

        mtd = p_return_attribution(window=1, annualize=False).rename(
            columns={"CTR": "MTD"})
        qtd = p_return_attribution(window=qtd_window, annualize=False).rename(
            columns={"CTR": "QTD"})
        ytd = p_return_attribution(window=ytd_window, annualize=False).rename(
            columns={"CTR": "YTD"})
        ttm = p_return_attribution(window=12, annualize=False).rename(
            columns={"CTR": "TTM"})
        t3y = p_return_attribution(window=36, annualize=True).rename(
            columns={"CTR": "T3Y"})
        t5y = p_return_attribution(window=60, annualize=True).rename(
            columns={"CTR": "T5Y"})
        t10y = p_return_attribution(window=120, annualize=True).rename(
            columns={"CTR": "T10Y"})

        # TODO only fill na if some non na's
        summary = factor_group_index.merge(mtd,
                                           left_index=True,
                                           right_index=True,
                                           how="left")
        summary = summary.merge(qtd, left_index=True, right_index=True, how="left")
        summary = summary.merge(ytd, left_index=True, right_index=True, how="left")
        summary = summary.merge(ttm, left_index=True, right_index=True, how="left")
        summary = summary.merge(t3y, left_index=True, right_index=True, how="left")
        summary = summary.merge(t5y, left_index=True, right_index=True, how="left")
        summary = summary.merge(t10y, left_index=True, right_index=True, how="left")
        summary.columns = ["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y"]
        summary = summary.T
        summary = summary.round(2)

        # fill na unless everything is NA
        summary[~summary.isna().all(axis=1)] = summary[~summary
                                                       .isna()
                                                       .all(axis=1)
                                                       ].fillna(0)
        return summary

    def _get_pba_summary(self):
        factor_group_index = pd.DataFrame(
            index=[
                "Beta",
                "Regional",
                "Industry",
                "Repay",
                "LS_Equity",
                "LS_Credit",
                "MacroRV",
                "Residual",
                "Fees",
                "Unallocated",
            ]
        )
        fund_pba_publics = self._fund_pba_publics.copy()
        fund_pba_privates = self._fund_pba_privates.copy()

        if fund_pba_publics.shape[0] > 1:
            mtd_publics = self._analytics.compute_periodic_return(
                ror=fund_pba_publics,
                period=PeriodicROR.MTD,
                as_of_date=self._as_of_date,
                method="arithmetic",
            )
            mtd_publics.name = "MTD - Publics"

            qtd_publics = self._analytics.compute_periodic_return(
                ror=fund_pba_publics,
                period=PeriodicROR.QTD,
                as_of_date=self._as_of_date,
                method="arithmetic",
            )
            qtd_publics.name = "QTD - Publics"

            ytd_publics = self._analytics.compute_periodic_return(
                ror=fund_pba_publics,
                period=PeriodicROR.YTD,
                as_of_date=self._as_of_date,
                method="arithmetic",
            )
            ytd_publics.name = "YTD - Publics"
        else:
            mtd_publics = pd.Series(
                index=factor_group_index.index,
                name="MTD - Publics",
                dtype="float64",
            )
            qtd_publics = pd.Series(
                index=factor_group_index.index,
                name="QTD - Publics",
                dtype="float64",
            )
            ytd_publics = pd.Series(
                index=factor_group_index.index,
                name="YTD - Publics",
                dtype="float64",
            )

        if fund_pba_privates.shape[0] > 1:
            mtd_privates = self._analytics.compute_periodic_return(
                ror=fund_pba_privates,
                period=PeriodicROR.MTD,
                as_of_date=self._as_of_date,
                method="arithmetic",
            )
            mtd_privates.name = "MTD - Privates"

            qtd_privates = self._analytics.compute_periodic_return(
                ror=fund_pba_privates,
                period=PeriodicROR.QTD,
                as_of_date=self._as_of_date,
                method="arithmetic",
            )
            qtd_privates.name = "QTD - Privates"

            ytd_privates = self._analytics.compute_periodic_return(
                ror=fund_pba_privates,
                period=PeriodicROR.YTD,
                as_of_date=self._as_of_date,
                method="arithmetic",
            )
            ytd_privates.name = "YTD - Privates"
        else:
            mtd_privates = pd.Series(
                index=factor_group_index.index,
                name="MTD - Privates",
                dtype="float64",
            )
            qtd_privates = pd.Series(
                index=factor_group_index.index,
                name="QTD - Privates",
                dtype="float64",
            )
            ytd_privates = pd.Series(
                index=factor_group_index.index,
                name="YTD - Privates",
                dtype="float64",
            )

        # TODO only fill na if some non na's
        summary = factor_group_index.merge(mtd_publics,
                                           left_index=True,
                                           right_index=True,
                                           how="left")
        summary = summary.merge(mtd_privates,
                                left_index=True,
                                right_index=True,
                                how="left")
        summary = summary.merge(qtd_publics,
                                left_index=True,
                                right_index=True,
                                how="left")
        summary = summary.merge(qtd_privates,
                                left_index=True,
                                right_index=True,
                                how="left")
        summary = summary.merge(ytd_publics,
                                left_index=True,
                                right_index=True,
                                how="left")
        summary = summary.merge(ytd_privates,
                                left_index=True,
                                right_index=True,
                                how="left")
        summary.columns = [
            "MTD - Publics",
            "MTD - Privates",
            "QTD - Publics",
            "QTD - Privates",
            "YTD - Publics",
            "YTD - Privates",
        ]
        summary = summary.T
        summary = summary.round(2)

        # fill na unless everything is NA
        summary[~summary.isna().all(axis=1)] = summary[
            ~summary.isna().all(axis=1)].fillna(0)
        return summary

    def build_exposure_summary(self):
        summary = self._get_exposure_summary(self._exposure)

        # nullify exposure where prior to track
        length_of_track = self._fund_returns.shape[0]

        if length_of_track < 12:
            summary.loc["1Y"] = np.nan

        if length_of_track < 36:
            summary.loc["3Y"] = np.nan

        if length_of_track < 60:
            summary.loc["5Y"] = np.nan

        if length_of_track < 120:
            summary.loc["10Y"] = np.nan

        return summary

    def build_rba_summary(self):
        if self._fund_rba.shape[0] > 0:

            rba = self._get_rba_summary()
            # summary = fund_returns.merge(rba, left_index=True, right_index=True)
            summary = rba.copy()
            summary.drop("10Y", inplace=True)

            rba_risk_decomp = self._fund_rba_risk_decomp.copy()
            valid_rba_index = summary.notna().any(axis=1)

            summary = summary.merge(
                rba_risk_decomp,
                left_index=True,
                right_index=True,
                how="left",
            )

            # Avg risk decomp includes partial periods so we filter to only show for periods with attribution
            summary.loc[~valid_rba_index] = np.nan

            # rba_r2 = self._fund_rba_adj_r_squared.copy()
            # summary = summary.merge(rba_r2, left_index=True, right_index=True, how='left')

        else:
            summary = pd.DataFrame(
                index=["MTD", "QTD", "YTD", "TTM", "3Y", "5Y"],
                columns=[
                    "SYSTEMATIC",
                    "REGION",
                    "INDUSTRY",
                    "REPAY",
                    "LS_EQUITY",
                    "LS_CREDIT",
                    "MACRO",
                    "NON_FACTOR_SECURITY_SELECTION",
                    "NON_FACTOR_OUTLIER_EFFECTS",
                    "SYSTEMATIC_RISK",
                    "X_ASSET_RISK",
                    "PUBLIC_LS_RISK",
                    "NON_FACTOR_RISK",
                ],
            )
        return summary

    def get_adj_r2(self):
        rba_r2 = self._fund_rba_adj_r_squared.copy()
        rba_r2 = rba_r2.loc["5Y"].to_frame()
        return rba_r2

    def build_pba_summary(self):
        if self._fund_pba_publics.shape[0] > 0:
            pba = self._get_pba_summary()
            # fund_returns = pd.DataFrame({'Total': pba.sum(axis=1, skipna=False)})
            # summary = fund_returns.merge(pba, left_index=True, right_index=True)
            summary = pba.copy()
        else:
            summary = pd.DataFrame(
                index=[
                    "MTD - Publics",
                    "MTD - Privates",
                    "QTD - Publics",
                    "QTD - Privates",
                    "YTD - Publics",
                    "YTD - Privates",
                ],
                columns=[
                    "Beta",
                    "Regional",
                    "Industry",
                    "Repay",
                    "LS_Equity",
                    "LS_Credit",
                    "MacroRV",
                    "Residual",
                    "Fees",
                    "Unallocated",
                ],
            )
        return summary

    def _get_fund_trailing_vol_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_vol = self._helper.get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_vol = self._helper.get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_vol = self._helper.get_trailing_vol(returns=returns, trailing_months=60)
        rolling_1_vol = self._helper.get_rolling_vol(returns=returns, trailing_months=12)
        trailing_5y_median_vol = self._helper.summarize_rolling_median(rolling_1_vol, trailing_months=60)

        stats = [
            trailing_1y_vol,
            trailing_3y_vol,
            trailing_5y_vol,
            trailing_5y_median_vol,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {"Vol": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_fund_trailing_beta_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_beta = self._helper.get_trailing_beta(returns=returns, trailing_months=12)
        trailing_3y_beta = self._helper.get_trailing_beta(returns=returns, trailing_months=36)
        trailing_5y_beta = self._helper.get_trailing_beta(returns=returns, trailing_months=60)

        returns_10y = returns[returns.index >= self._helper.sp500_return.index.min()]
        if returns_10y.shape[0] >= 12:
            rolling_1y_beta = self._helper.get_rolling_beta(returns=returns_10y, trailing_months=12)
            trailing_5y_median_beta = self._helper.summarize_rolling_median(rolling_1y_beta, trailing_months=60)
        else:
            trailing_5y_median_beta = pd.DataFrame()

        stats = [
            trailing_1y_beta,
            trailing_3y_beta,
            trailing_5y_beta,
            trailing_5y_median_beta,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {"Beta": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_fund_trailing_sharpe_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_sharpe = self._helper.get_trailing_sharpe(returns=returns, trailing_months=12)
        trailing_3y_sharpe = self._helper.get_trailing_sharpe(returns=returns, trailing_months=36)
        trailing_5y_sharpe = self._helper.get_trailing_sharpe(returns=returns, trailing_months=60)
        rolling_1y_sharpe = self._helper.get_rolling_sharpe_ratio(returns=returns, trailing_months=12)
        trailing_5y_median_sharpe = self._helper.summarize_rolling_median(rolling_1y_sharpe, trailing_months=60)

        stats = [
            trailing_1y_sharpe,
            trailing_3y_sharpe,
            trailing_5y_sharpe,
            trailing_5y_median_sharpe,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {"Sharpe": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_fund_trailing_batting_average_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_batting_avg = self._helper.get_trailing_batting_avg(returns=returns, trailing_months=12)
        trailing_3y_batting_avg = self._helper.get_trailing_batting_avg(returns=returns, trailing_months=36)
        trailing_5y_batting_avg = self._helper.get_trailing_batting_avg(returns=returns, trailing_months=60)
        rolling_1y_batting_avg = self._helper.get_rolling_batting_avg(returns=returns, trailing_months=12)
        trailing_5y_median_batting_avg = self._helper.summarize_rolling_median(rolling_1y_batting_avg, trailing_months=60)

        stats = [
            trailing_1y_batting_avg,
            trailing_3y_batting_avg,
            trailing_5y_batting_avg,
            trailing_5y_median_batting_avg,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {"BattingAvg": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_fund_trailing_win_loss_ratio_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_win_loss = self._helper.get_trailing_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_3y_win_loss = self._helper.get_trailing_win_loss_ratio(returns=returns, trailing_months=36)
        trailing_5y_win_loss = self._helper.get_trailing_win_loss_ratio(returns=returns, trailing_months=60)
        rolling_1y_win_loss = self._helper.get_rolling_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_5y_median_win_loss = self._helper.summarize_rolling_median(rolling_1y_win_loss, trailing_months=60)

        stats = [
            trailing_1y_win_loss,
            trailing_3y_win_loss,
            trailing_5y_win_loss,
            trailing_5y_median_win_loss,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {"WinLoss": [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["TTM", "3Y", "5Y", "5YMedian"],
        )

        return summary

    def _get_fund_rolling_return_summary(self):
        returns = self._fund_returns.copy()
        rolling_12m_returns = self._helper.get_rolling_return(returns=returns, trailing_months=12)
        rolling_1y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=12)
        rolling_3y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=36)
        rolling_5y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=60)

        summary = pd.concat(
            [
                rolling_1y_summary.T,
                rolling_3y_summary.T,
                rolling_5y_summary.T,
            ]
        )
        summary.index = ["TTM", "3Y", "5Y"]

        return summary

    def _get_fund_rolling_sharpe_summary(self):
        returns = self._fund_returns.copy()
        rolling_12m_sharpes = self._helper.get_rolling_sharpe_ratio(returns=returns, trailing_months=12)
        rolling_1y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=12)
        rolling_3y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=36)
        rolling_5y_summary = self._helper.summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=60)

        summary = pd.concat(
            [
                rolling_1y_summary.T,
                rolling_3y_summary.T,
                rolling_5y_summary.T,
            ]
        )
        summary.index = ["TTM", "3Y", "5Y"]

        return summary

    def build_conditional_fund_return_summary(self):
        market_percentiles = ['< 10th', '10th - 25th', '25th - 50th',
                              '50th - 75th', '75th - 90th', '> 90th']
        column_headings = ["Excess 25%", "Excess mean", "Excess 75%",
                           'Excess Ptile - 25%', 'Excess Ptile - Mean', 'Excess Ptile - 75%', "Excess count"]

        if self._fund_returns.shape[0] < 36 or self._primary_peer_group is None:
            return pd.DataFrame(columns=column_headings, index=market_percentiles)

        bmrk = self._market_scenarios_3y.columns[0]
        rors = self._fund_returns.merge(self._market_returns_monthly, left_index=True, right_index=True)

        if bmrk == 'MOVE Index':
            beta = 0
        elif bmrk in self._abs_bmrk_betas.index:
            beta = self._abs_bmrk_betas.loc[bmrk].squeeze()
        else:
            beta = scipy.stats.linregress(x=rors[bmrk], y=rors[self._fund_name])[0]
            beta = round(max(min(beta, 1.5), 0.1), 1)

        beta_adj_index = rors[bmrk] * beta
        excess_return = rors[self._fund_name] - beta_adj_index
        excess_return = excess_return.to_frame('Excess')
        total_return = rors[self._fund_name].to_frame('Total')

        fund_returns = total_return.merge(excess_return, left_index=True, right_index=True)

        rolling_fund_returns = self._analytics.compute_trailing_return(ror=fund_returns,
                                                                       window=36,
                                                                       as_of_date=excess_return.index.max().date(),
                                                                       method='geometric',
                                                                       periodicity=Periodicity.Monthly,
                                                                       annualize=True,
                                                                       include_history=True)

        rolling_rors = rolling_fund_returns.merge(self._market_scenarios_3y, left_index=True, right_index=True)

        prim_peer_ret = pd.DataFrame(self._primary_peer_returns)
        prim_peer_ret.rename(columns={'0': self._primary_peer_group})

        ptile_25, ptile_mean, ptile_75 = np.percentile(
            rolling_rors['Excess'], 25), np.mean(rolling_rors['Excess']), np.percentile(rolling_rors['Excess'], 75)

        const_ret = PerformanceQualityPeerLevelAnalytics(peer_group=self._primary_peer_group)._constituent_returns
        bmrk_ret = PerformanceQualityPeerLevelAnalytics(peer_group=self._primary_peer_group)._peer_arb_benchmark_returns
        # const_ret=self._primary_peer_constituent_returns
        # bmrk_ret=self._abs_bmrk_returns
        market_scenarios, conditional_ptile_summary = \
            generate_peer_conditional_excess_returns(peer_returns=const_ret,
                                                     benchmark_returns=bmrk_ret)

        rolling_excess, _ = _compute_rolling_excess_metrics(
            const_ret, bmrk_ret, percentiles=[10, 25, 50, 75, 90], window=36)
        passive_bmrk = bmrk_ret.columns[0]
        if passive_bmrk == 'MOVE Index':
            rolling_3y_bmrk = bmrk_ret.rolling(36).mean().dropna()
        else:
            rolling_3y_bmrk = self._analytics.compute_trailing_return(
                ror=bmrk_ret,
                window=36,
                as_of_date=bmrk_ret.index.max().date(),
                method='geometric',
                periodicity=Periodicity.Monthly,
                annualize=True,
                include_history=True)

        spx_ror_cutoffs = [-np.inf] + [np.inf]
        rolling_3y_bmrk['MarketScenario'] = pd.cut(
            rolling_3y_bmrk.iloc[:, 0],
            bins=spx_ror_cutoffs,
            labels=['composite'])

        conditional_ptiles = rolling_3y_bmrk.merge(rolling_excess, how='left', left_index=True, right_index=True)
        conditional_ptile_summary = conditional_ptiles.groupby('MarketScenario').mean()
        conditional_ptile_summary = conditional_ptile_summary.round(2)
        conditional_ptile_summary = conditional_ptile_summary.iloc[:1]
        condl_list = conditional_ptile_summary.loc['composite', :].values.flatten().tolist()
        condl_list = condl_list[1:]

        ind = [10, 25, 50, 75, 90]
        peer_excess = {'peer_excess': condl_list}
        df_peer = pd.DataFrame(peer_excess, index=ind)
        fund_excess = {'fund_excess': [ptile_25, ptile_mean, ptile_75]}
        df_fund = pd.DataFrame(fund_excess)
        df_fund.columns = ['fund_excess']

        abs_diff = np.abs(df_fund['fund_excess'].values[:, np.newaxis] - df_peer['peer_excess'].values)
        min_ind = np.argmin(abs_diff, axis=1)
        df_fund['fund_peer_perc'] = df_peer.index[min_ind]

        peer_vals = df_peer['peer_excess'].values
        fund_vals = df_fund['fund_excess'].values
        fund_peer_perc_vals = df_fund['fund_peer_perc'].values

        fund_peer_perc_strs = ['' if str(x) == 'nan' else str(int(x)) + 'th' for x in fund_peer_perc_vals]

        composite_condl = list(peer_vals) + list(fund_vals) + list(fund_peer_perc_strs)

        summary_stats = rolling_rors.groupby('MarketScenario')[['Excess']].describe()
        summary_stats.columns = [' '.join(col).strip() for col in summary_stats.columns.values]
        summary_stats = summary_stats[["Excess 25%", "Excess mean", "Excess 75%", "Excess count"]]
        summary_stats = summary_stats.reindex(market_percentiles)

        fund_25 = self._condl_peer_excess_returns.sub(summary_stats['Excess 25%'], axis=0).abs().idxmin(axis=1)
        fund_mean = self._condl_peer_excess_returns.sub(summary_stats['Excess mean'], axis=0).abs().idxmin(axis=1)
        fund_75 = self._condl_peer_excess_returns.sub(summary_stats['Excess 75%'], axis=0).abs().idxmin(axis=1)

        summary_stats['Excess Ptile - 25%'] = ['' if str(x) == 'nan' else str(int(x)) + 'th' for x in fund_25]
        summary_stats['Excess Ptile - Mean'] = ['' if str(x) == 'nan' else str(int(x)) + 'th' for x in fund_mean]
        summary_stats['Excess Ptile - 75%'] = ['' if str(x) == 'nan' else str(int(x)) + 'th' for x in fund_75]

        summary_stats = summary_stats[column_headings]

        composite_obs = summary_stats.iloc[:, -1].sum()
        composite_condl = composite_condl + [composite_obs]
        composite_df = pd.DataFrame([composite_condl])

        return summary_stats, composite_df

    def build_performance_stability_fund_summary(self):
        if self._fund_returns.shape[0] == 0:
            return pd.DataFrame(
                columns=[
                    "Vol",
                    "Beta",
                    "Sharpe",
                    "BattingAvg",
                    "WinLoss",
                    "Return_min",
                    "Return_25%",
                    "Return_75%",
                    "Return_max",
                    "Sharpe_min",
                    "Sharpe_25%",
                    "Sharpe_75%",
                    "Sharpe_max",
                ],
                index=["TTM", "3y", "5Y", "5Y Median"],
            )
        vol = self._get_fund_trailing_vol_summary()
        beta = self._get_fund_trailing_beta_summary()
        sharpe = self._get_fund_trailing_sharpe_summary()
        batting_avg = self._get_fund_trailing_batting_average_summary()
        win_loss = self._get_fund_trailing_win_loss_ratio_summary()

        rolling_returns = self._get_fund_rolling_return_summary()
        rolling_returns.columns = ["Return_"] + rolling_returns.columns

        rolling_sharpes = self._get_fund_rolling_sharpe_summary()
        rolling_sharpes.columns = ["Sharpe_"] + rolling_sharpes.columns

        summary = vol.merge(beta, left_index=True, right_index=True, how="left")
        summary = summary.merge(sharpe, left_index=True, right_index=True, how="left")
        summary = summary.merge(batting_avg, left_index=True, right_index=True, how="left")
        summary = summary.merge(win_loss, left_index=True, right_index=True, how="left")
        summary = summary.merge(rolling_returns, left_index=True, right_index=True, how="left")
        summary = summary.merge(rolling_sharpes, left_index=True, right_index=True, how="left")

        summary = summary[
            [
                "Vol",
                "Beta",
                "Sharpe",
                "BattingAvg",
                "WinLoss",
                "Return_min",
                "Return_25%",
                "Return_75%",
                "Return_max",
                "Sharpe_min",
                "Sharpe_25%",
                "Sharpe_75%",
                "Sharpe_max",
            ]
        ]
        return summary

    def build_performance_stability_peer_summary(self):
        if self._primary_peer_group is not None:
            summary = pd.read_json(
                self._primary_peer_analytics["performance_stability_peer_summary"],
                orient="index",
            )
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

    def build_shortfall_summary(self):
        returns = self._fund_returns.copy()
        if returns.shape[0] > 0:
            drawdown = self._analytics.compute_max_drawdown(
                ror=returns,
                window=12,
                as_of_date=self._as_of_date,
                periodicity=Periodicity.Monthly,
            )

            drawdown = drawdown.squeeze()
        else:
            drawdown = None

        trigger = self._fle_scl.copy()

        if drawdown is not None:
            drawdown = round(drawdown, 2)

            if drawdown < trigger:
                pass_fail = "Fail"
            else:
                pass_fail = "Pass"
        else:
            pass_fail = ""
            drawdown = ""

        summary = pd.DataFrame(
            {
                "Trigger": trigger,
                "Drawdown": drawdown,
                "Pass/Fail": pass_fail,
            },
            index=["SCL"],
        )
        return summary

    def build_outsize_positive_performance_summary(self):
        returns = self._fund_returns.copy()
        if returns.shape[0] > 0:
            drawup = self._analytics.compute_max_trough_to_peak(
                ror=returns,
                window=12,
                as_of_date=self._as_of_date,
                periodicity=Periodicity.Monthly,
            )

            drawup = drawup.squeeze()
        else:
            drawup = None

        trigger = np.absolute(self._fle_scl.copy())

        if drawup is not None:
            drawup = round(drawup, 2)

            if drawup > trigger:
                pass_fail = "Fail"
            else:
                pass_fail = "Pass"
        else:
            pass_fail = ""
            drawup = ""

        summary = pd.DataFrame(
            {
                "Trigger": trigger,
                "Drawdown": drawup,
                "Pass/Fail": pass_fail,
            },
            index=["SCL"],
        )
        return summary

    def build_risk_model_expectations_summary(self):
        summary = pd.DataFrame(
            {
                "Expectations": [
                    self._risk_model_expected_return,
                    self._risk_model_expected_vol,
                ]
            },
            index=["ExpectedReturn", "ExpectedVolatility"],
        )
        return summary

    def build_risk_model_implied_forwards_summary(self):
        number_sims = self._fund_distributions.shape[0]

        if self._fund_returns.shape[0] < 12 or number_sims == 0:
            summary = pd.DataFrame(columns=["Forwards"], index=["TTM_Ptile_vs_Expected", "ForwardReturn"])
            return summary

        exp_return = self._risk_model_expected_return

        ttm_return = self._analytics.compute_trailing_return(
            ror=self._fund_returns,
            window=12,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )
        ttm_return = ttm_return.squeeze()

        ttm_plus_2y_exp = ((1 + ttm_return) * (1 + exp_return) * (1 + exp_return)) ** (1 / 3) - 1
        ttm_plus_2y_exp = ttm_plus_2y_exp.round(2)

        ttm_ptile = (self._fund_distributions < ttm_plus_2y_exp).sum().squeeze() / number_sims
        ttm_ptile = ttm_ptile.round(2)

        shrinkage = 0.25
        shrunk_ptile = (ttm_ptile * (1 - shrinkage)) + (0.5 * shrinkage)
        distribution_index = int(round(shrunk_ptile * number_sims, 0))
        sorted_distribution = sorted(self._fund_distributions.squeeze())
        implied_forward_3y = round(sorted_distribution[distribution_index], 2)

        implied_forward_2y = (((1 + implied_forward_3y) ** 3) / (1 + ttm_return)) ** (1 / 2) - 1
        # implied_3y_check = ((1 + implied_forward_2y) * (1 + implied_forward_2y) * (1 + ttm_return)) ** (1 / 3) - 1
        # round(implied_3y_check, 2) == implied_forward_3y

        implied_forward_2y = round(implied_forward_2y, 2)
        summary = pd.DataFrame(
            {
                "Forwards": [
                    ttm_ptile * 100,
                    implied_forward_2y,
                ]
            },
            index=["TTM_Ptile_vs_Expected", "ForwardReturn"],
        )
        return summary

    def build_monthly_performance_summary(self):
        end_year = self._as_of_date.year
        years = [x for x in range(end_year - 52, end_year + 1)]
        months = [x for x in range(1, 13)]

        if self._fund_returns.shape[0] == 0:
            return pd.DataFrame(columns=["Month", "Year"] + months + ["YTD"], index=years)

        returns = self._fund_returns.copy()
        returns["Month"] = returns.index.month
        returns["Year"] = returns.index.year

        # pivot long to wide
        monthly_returns = returns.pivot(index=["Year"], columns=["Month"])
        monthly_returns = monthly_returns.reindex(years, axis=0)
        monthly_returns.columns = monthly_returns.columns.droplevel(0)
        monthly_returns = monthly_returns.reindex(months, axis=1)
        monthly_returns = monthly_returns.sort_values("Year", ascending=False)

        # append ytd returns
        ytd_returns = monthly_returns.apply(lambda x: np.prod(1 + x) - 1, axis=1)
        ytd_index = ~monthly_returns.isna().all(axis=1)
        monthly_returns["YTD"] = None
        monthly_returns.loc[ytd_index, "YTD"] = ytd_returns[ytd_index]

        monthly_returns = 100 * monthly_returns.astype(float).round(3)
        monthly_returns = monthly_returns.reset_index()

        monthly_returns.loc[~ytd_index.values, "Year"] = None

        return monthly_returns

    def build_net_exposure_timeseries_summary(self):
        end_year = self._as_of_date.year
        years = [x for x in range(end_year - 52, end_year + 1)]
        months = [x for x in range(1, 13)]

        if self._fund_net_exposures.shape[0] == 0:
            return pd.DataFrame(columns=["Month", "Year"] + months + ["YTD"], index=years)

        net_exps = self._fund_net_exposures.copy()
        net_exps['Month'] = net_exps.index.month
        net_exps['Year'] = net_exps.index.year
        # net_exps.index=net_exps.index.map(ast.literal_eval)
        # net_exps['Year']=[index[0] for index in net_exps.index]
        # net_exps['Month']=[index[1] for index in net_exps.index]

        # pivot long to wide
        monthly_net_exps = net_exps.pivot(index=["Year"], columns=["Month"])
        monthly_net_exps = monthly_net_exps.reindex(years, axis=0)
        monthly_net_exps.columns = monthly_net_exps.columns.droplevel(0)
        monthly_net_exps = monthly_net_exps.reindex(months, axis=1)
        monthly_net_exps = monthly_net_exps.sort_values("Year", ascending=False)

        monthly_net_exps = 100 * monthly_net_exps.astype(float).round(3)
        monthly_net_exps = monthly_net_exps.reset_index()

        ytd_index = ~monthly_net_exps.isna().all(axis=1)
        monthly_net_exps.loc[~ytd_index.values, "Year"] = None
        monthly_net_exps.loc[monthly_net_exps.drop('Year', axis=1).isna().all(axis=1), 'Year'] = None

        return monthly_net_exps

    def build_exposure_timeseries_summary(self):
        end_year = self._as_of_date.year
        years = [x for x in range(end_year - 52, end_year + 1)]
        months = [x for x in range(1, 13)]

        if self._fund_gross_exposures.shape[0] == 0:
            return pd.DataFrame(columns=["Month", "Year"] + months + ["YTD"], index=years)

        gross_exps = self._fund_gross_exposures.copy()
        gross_exps['Month'] = gross_exps.index.month
        gross_exps['Year'] = gross_exps.index.year
        # gross_exps.index=gross_exps.index.map(ast.literal_eval)
        # gross_exps['Year']=[index[0] for index in gross_exps.index]
        # gross_exps['Month']=[index[1] for index in gross_exps.index]

        # pivot long to wide
        monthly_gross_exps = gross_exps.pivot(index=["Year"], columns=["Month"])
        monthly_gross_exps = monthly_gross_exps.reindex(years, axis=0)
        monthly_gross_exps.columns = monthly_gross_exps.columns.droplevel(0)
        monthly_gross_exps = monthly_gross_exps.reindex(months, axis=1)
        monthly_gross_exps = monthly_gross_exps.sort_values("Year", ascending=False)

        monthly_gross_exps = 100 * monthly_gross_exps.astype(float).round(3)
        monthly_gross_exps = monthly_gross_exps.reset_index()

        ytd_index = ~monthly_gross_exps.isna().all(axis=1)
        monthly_gross_exps.loc[~ytd_index.values, "Year"] = None
        monthly_gross_exps.loc[monthly_gross_exps.drop('Year', axis=1).isna().all(axis=1), 'Year'] = None

        return monthly_gross_exps

    def _validate_inputs(self):
        if self._fund_returns.shape[0] == 0:
            return False
        else:
            return True

    def generate_performance_quality_report(self):
        # if not self._validate_inputs():
        #     return 'Invalid inputs'

        logging.info("Generating report for: " + self._fund_name)
        header_info = self.get_header_info()

        shortfall_summary = self.build_shortfall_summary()
        outsize_positive_performance_summary = self.build_outsize_positive_performance_summary()
        return_summary = self.build_benchmark_summary()
        conditional_fund_return_summary = self.build_conditional_fund_return_summary()[0]
        conditional_composite_summary = self.build_conditional_fund_return_summary()[1]
        constituent_count_summary = self.build_constituent_count_summary()
        absolute_return_benchmark = self.get_absolute_return_benchmark()
        peer_group_heading = self.get_peer_group_heading()
        eurekahedge_benchmark_heading = self.get_eurekahedge_benchmark_heading()
        peer_ptile_1_heading = self.get_peer_ptile_1_heading()
        # peer_ptile_2_heading = self.get_peer_ptile_2_heading()

        rba_summary = self.build_rba_summary()
        adj_r2 = self.get_adj_r2()
        pba_summary = self.build_pba_summary()
        pba_mtd = pba_summary.loc[["MTD - Publics", "MTD - Privates"]]
        pba_qtd = pba_summary.loc[["QTD - Publics", "QTD - Privates"]]
        pba_ytd = pba_summary.loc[["YTD - Publics", "YTD - Privates"]]

        performance_stability_fund_summary = self.build_performance_stability_fund_summary()
        performance_stability_peer_summary = self.build_performance_stability_peer_summary()

        # shortfall_summary = self.build_shortfall_summary()
        risk_model_expectations = self.build_risk_model_expectations_summary()
        risk_model_implied_forwards = self.build_risk_model_implied_forwards_summary()

        exposure_summary = self.build_exposure_summary()
        latest_exposure_heading = self.get_latest_exposure_heading()

        monthly_performance_summary = self.build_monthly_performance_summary()

        exposure_timeseries_summary = self.build_exposure_timeseries_summary()

        net_exposure_timeseries_summary = self.build_net_exposure_timeseries_summary()

        logging.info("Report summary data generated for: " + self._fund_name)

        input_data = {
            "header_info_1": header_info,
            "header_info_2": header_info,
            "header_info_3": header_info,
            "header_info_4": header_info,
            "header_info_5": header_info,
            "benchmark_summary": return_summary,
            "constituent_count_summary": constituent_count_summary,
            "absolute_return_benchmark": absolute_return_benchmark,
            "peer_group_heading": peer_group_heading,
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading,
            "peer_ptile_1_heading": peer_ptile_1_heading,
            # "peer_ptile_2_heading": peer_ptile_2_heading,
            "performance_stability_fund_summary": performance_stability_fund_summary,
            "performance_stability_peer_summary": performance_stability_peer_summary,
            "rba_summary": rba_summary,
            "adj_r2": adj_r2,
            "pba_mtd": pba_mtd,
            "pba_qtd": pba_qtd,
            "pba_ytd": pba_ytd,
            "shortfall_summary": shortfall_summary,
            "outsize_positive_performance_summary": outsize_positive_performance_summary,
            "risk_model_expectations": risk_model_expectations,
            "exposure_summary": exposure_summary,
            "latest_exposure_heading": latest_exposure_heading,
            "monthly_performance_summary": monthly_performance_summary,
            "monthly_exposure_summary": exposure_timeseries_summary,
            "net_exposure_timeseries_summary": net_exposure_timeseries_summary,
            "condl_mkt_bmrk": self._condl_mkt_bmrk,
            "condl_mkt_return": self._condl_mkt_return,
            "condl_peer_excess_returns": self._condl_peer_excess_returns,
            "condl_peer_heading": self._condl_peer_heading,
            "condl_fund_return_summary": conditional_fund_return_summary,
            "condl_composite_summary": conditional_composite_summary,
            "risk_model_forwards": risk_model_implied_forwards,
        }

        input_data_json = {
            "header_info_1": header_info.to_json(orient="index"),
            "header_info_2": header_info.to_json(orient="index"),
            "header_info_3": header_info.to_json(orient="index"),
            "header_info_4": header_info.to_json(orient="index"),
            "header_info_5": header_info.to_json(orient="index"),
            "benchmark_summary": return_summary.to_json(orient="index"),
            "constituent_count_summary": constituent_count_summary.to_json(orient="index"),
            "absolute_return_benchmark": absolute_return_benchmark.to_json(orient="index"),
            "peer_group_heading": peer_group_heading.to_json(orient="index"),
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading.to_json(orient="index"),
            "peer_ptile_1_heading": peer_ptile_1_heading.to_json(orient="index"),
            # "peer_ptile_2_heading": peer_ptile_2_heading.to_json(orient="index"),
            "performance_stability_fund_summary": performance_stability_fund_summary.to_json(orient="index"),
            "performance_stability_peer_summary": performance_stability_peer_summary.to_json(orient="index"),
            "rba_summary": rba_summary.to_json(orient="index"),
            "adj_r2": adj_r2.to_json(orient="index"),
            "pba_mtd": pba_mtd.to_json(orient="index"),
            "pba_qtd": pba_qtd.to_json(orient="index"),
            "pba_ytd": pba_ytd.to_json(orient="index"),
            "shortfall_summary": shortfall_summary.to_json(orient="index"),
            "outsize_positive_performance_summary": outsize_positive_performance_summary.to_json(orient="index"),
            "risk_model_expectations": risk_model_expectations.to_json(orient="index"),
            "exposure_summary": exposure_summary.to_json(orient="index"),
            "latest_exposure_heading": latest_exposure_heading.to_json(orient="index"),
            "monthly_performance_summary": monthly_performance_summary.to_json(orient="index"),
            "monthly_exposure_summary": exposure_timeseries_summary.to_json(orient="index"),
            "net_exposure_timeseries_summary": net_exposure_timeseries_summary.to_json(orient="index"),
            "condl_mkt_bmrk": self._condl_mkt_bmrk.to_json(orient="index"),
            "condl_mkt_return": self._condl_mkt_return.to_json(orient="index"),
            "condl_peer_excess_returns": self._condl_peer_excess_returns.to_json(orient="index"),
            "condl_peer_heading": self._condl_peer_heading.to_json(orient="index"),
            "condl_fund_return_summary": conditional_fund_return_summary.to_json(orient="index"),
            "condl_composite_summary": conditional_composite_summary.to_json(orient="index"),
            "risk_model_forwards": risk_model_implied_forwards.to_json(orient="index"),
        }

        data_to_write = json.dumps(input_data_json)
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            self._fund_name.replace("/", "") + "_fund_" + as_of_date + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

        logging.info("JSON stored to DataLake for: " + self._fund_name)

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        pub_id = self._pub_investment_group_id.item()

        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="PFUND_PerformanceQuality_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name=self._fund_name,
                entity_display_name=self._fund_name.replace("/", ""),
                entity_ids=[pub_id],
                entity_source=DaoSource.PubDwh,
                report_name="ARS Performance Quality",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                aggregate_intervals=AggregateInterval.MTD,
                # output_dir="cleansed/investmentsreporting/printedexcels/",
                # report_output_source=DaoSource.DataLake,
            )

        logging.info("Excel stored to DataLake for: " + self._fund_name)

    def run(self, **kwargs):
        try:
            self.generate_performance_quality_report()
            return f"{self._fund_name} Complete"
        except Exception as e:
            raise RuntimeError(f"Failed for {self._fund_name}") from e


if __name__ == "__main__":
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

    with Scenario(dao=runner, as_of_date=dt.date(2022, 10, 31)).context():
        for fund in ['Skye', 'Citadel', 'Element', 'D1 Capital']:
            # for fund in ['Skye', 'Element']:
            PerformanceQualityReport(fund_name=fund).execute()
