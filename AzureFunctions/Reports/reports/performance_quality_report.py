import json
import logging
import pandas as pd
import ast
import numpy as np
import datetime as dt
from functools import partial
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval, ReportVertical,
)
from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.Scenario.scenario import Scenario
from gcm.inv.quantlib.enum_source import PeriodicROR, Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import (
    AggregateFromDaily,
)
from gcm.inv.reporting.core.reporting_runner_base import (
    ReportingRunnerBase,
)


class PerformanceQualityReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, fund_name):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._analytics = Analytics()
        self._fund_name = fund_name
        self._fund_inputs_cache = None
        self._fund_dimn_cache = None
        self._fund_returns_cache = None
        self._abs_bmrk_returns_cache = None
        self._exposure_cache = None
        self._rba_cache = None
        self._rba_risk_decomp_cache = None
        self._rba_adj_r2_cache = None
        self._pba_publics_cache = None
        self._pba_privates_cache = None
        self._primary_peer_inputs_cache = None
        self._secondary_peer_inputs_cache = None
        self._eurekahedge_inputs_cache = None
        self._eurekahedge200_inputs_cache = None
        self._market_factor_inputs_cache = None
        self._market_factor_returns_cache = None
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/performancequality"
        self._summary_data_location = "raw/investmentsreporting/summarydata/performancequality"

    def _download_inputs(self, location, file_path) -> dict:
        read_params = AzureDataLakeDao.create_get_data_params(
            location,
            file_path,
            retry=False,
        )
        file = self._runner.execute(
            params=read_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(read_params),
        )
        inputs = json.loads(file.content)
        return inputs

    @property
    def _fund_inputs(self):
        if self._fund_inputs_cache is None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = self._fund_name.replace("/", "") + "_fund_inputs_" + asofdate + ".json"
            self._fund_inputs_cache = self._download_inputs(location=self._underlying_data_location, file_path=file)
        return self._fund_inputs_cache

    @property
    def _fund_dimn(self):
        if self._fund_dimn_cache is None:
            self._fund_dimn_cache = pd.read_json(self._fund_inputs["fund_dimn"], orient="index")
        return self._fund_dimn_cache

    @property
    def _fund_returns(self):
        if self._fund_returns_cache is None:
            self._fund_returns_cache = pd.read_json(self._fund_inputs["fund_returns"], orient="index")
            self._fund_returns_cache = self._fund_returns_cache.sort_index()

        if any(self._fund_returns_cache.columns == self._fund_name):
            return self._fund_returns_cache[self._fund_name].to_frame()
        else:
            return pd.DataFrame()

    @property
    def _abs_bmrk_returns(self):
        if self._abs_bmrk_returns_cache is None:
            returns = pd.read_json(self._fund_inputs["abs_bmrk_returns"], orient="index")
            if len(returns) > 1:
                self._abs_bmrk_returns_cache = AggregateFromDaily().transform(
                    data=returns,
                    method="geometric",
                    period=Periodicity.Monthly,
                )
            else:
                self._abs_bmrk_returns_cache = pd.DataFrame()

        if any(self._fund_id.squeeze() == list(self._abs_bmrk_returns_cache.columns)):
            returns = self._abs_bmrk_returns_cache[self._fund_id].squeeze()
        else:
            returns = pd.DataFrame()
        return returns

    @property
    def _all_exposure(self):
        if self._exposure_cache is None:
            latest = pd.read_json(self._fund_inputs["exposure_latest"], orient="index")
            latest["Period"] = "Latest"
            three = pd.read_json(self._fund_inputs["exposure_3y"], orient="index")
            three["Period"] = "3Y"
            five = pd.read_json(self._fund_inputs["exposure_5y"], orient="index")
            five["Period"] = "5Y"
            ten = pd.read_json(self._fund_inputs["exposure_10y"], orient="index")
            ten["Period"] = "10Y"
            all_exposure = pd.concat([latest, three, five, ten])
            self._exposure_cache = all_exposure
        return self._exposure_cache

    @property
    def _exposure(self):
        if self._all_exposure.shape[0] > 0:
            exposure = self._all_exposure[self._all_exposure["InvestmentGroupName"] == self._fund_name]
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

    @property
    def _all_rba(self):
        if self._rba_cache is None:
            rba = pd.read_json(self._fund_inputs["rba"], orient="index")
            rba_columns = [ast.literal_eval(x) for x in rba.columns]
            rba_columns = pd.MultiIndex.from_tuples(rba_columns, names=["FactorGroup1", "AggLevel"])
            rba.columns = rba_columns
            self._rba_cache = rba
        return self._rba_cache

    @property
    def _all_rba_risk_decomp(self):
        if self._rba_risk_decomp_cache is None:
            self._rba_risk_decomp_cache = pd.read_json(self._fund_inputs["rba_risk_decomp"], orient="index")
        return self._rba_risk_decomp_cache

    @property
    def _all_rba_adj_r_squared(self):
        if self._rba_adj_r2_cache is None:
            self._rba_adj_r2_cache = pd.read_json(self._fund_inputs["rba_adj_r_squared"], orient="index")
        return self._rba_adj_r2_cache

    @property
    def _all_pba_publics(self):
        if self._pba_publics_cache is None:
            pba = pd.read_json(self._fund_inputs["pba_publics"], orient="index")
            pba_columns = [ast.literal_eval(x) for x in pba.columns]
            pba_columns = pd.MultiIndex.from_tuples(pba_columns, names=["FactorGroup1", "InvestmentGroupId"])
            pba.columns = pba_columns
            self._pba_publics_cache = pba
        return self._pba_publics_cache

    @property
    def _all_pba_privates(self):
        if self._pba_privates_cache is None:
            pba = pd.read_json(self._fund_inputs["pba_privates"], orient="index")
            pba_columns = [ast.literal_eval(x) for x in pba.columns]
            pba_columns = pd.MultiIndex.from_tuples(pba_columns, names=["FactorGroup1", "InvestmentGroupId"])
            pba.columns = pba_columns
            self._pba_privates_cache = pba
        return self._pba_privates_cache

    @property
    def _primary_peer_inputs(self):
        if self._primary_peer_inputs_cache is None and self._primary_peer_group is not None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = self._primary_peer_group.replace("/", "") + "_peer_inputs_" + asofdate + ".json"
            self._primary_peer_inputs_cache = self._download_inputs(
                location=self._underlying_data_location, file_path=file
            )
        return self._primary_peer_inputs_cache

    @property
    def _secondary_peer_inputs(self):
        if self._secondary_peer_inputs_cache is None and self._secondary_peer_group is not None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = self._secondary_peer_group.replace("/", "") + "_peer_inputs_" + asofdate + ".json"
            self._secondary_peer_inputs_cache = self._download_inputs(
                location=self._underlying_data_location, file_path=file
            )
        return self._secondary_peer_inputs_cache

    @property
    def _primary_peer_returns(self):
        returns = self._primary_peer_inputs
        if returns is not None:
            returns = pd.read_json(returns["gcm_peer_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series()

    @property
    def _secondary_peer_returns(self):
        returns = self._secondary_peer_inputs
        if returns is not None:
            returns = pd.read_json(returns["gcm_peer_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series()

    @property
    def _primary_peer_constituent_returns(self):
        returns = self._primary_peer_inputs
        if returns is not None:
            returns = pd.read_json(returns["gcm_peer_constituent_returns"], orient="index")
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(
                returns_columns,
                names=["PeerGroupName", "SourceInvestmentId"],
            )
            returns.columns = returns_columns
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
        return returns

    @property
    def _secondary_peer_constituent_returns(self):
        returns = self._secondary_peer_inputs
        if returns is not None:
            returns = pd.read_json(returns["gcm_peer_constituent_returns"], orient="index")
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(
                returns_columns,
                names=["PeerGroupName", "SourceInvestmentId"],
            )
            returns.columns = returns_columns
            returns = returns.droplevel(0, axis=1)
        else:
            returns = pd.DataFrame()
        return returns

    @property
    def _eurekahedge_inputs(self):
        if self._eurekahedge_inputs_cache is None and self._eurekahedge_benchmark is not None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = self._eurekahedge_benchmark.replace("/", "") + "_eurekahedge_inputs_" + asofdate + ".json"
            self._eurekahedge_inputs_cache = self._download_inputs(
                location=self._underlying_data_location, file_path=file
            )
        return self._eurekahedge_inputs_cache

    @property
    def _eurekahedge200_inputs(self):
        if self._eurekahedge200_inputs_cache is None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = "Eurekahedge Institutional 200" + "_eurekahedge_inputs_" + asofdate + ".json"
            self._eurekahedge200_inputs_cache = self._download_inputs(
                location=self._underlying_data_location, file_path=file
            )
        return self._eurekahedge200_inputs_cache

    @property
    def _ehi50_returns(self):
        returns = self._eurekahedge_inputs
        if returns is not None:
            returns = pd.read_json(returns["eurekahedge_returns"], orient="index")
            return returns.squeeze()
        else:
            return pd.Series()

    @property
    def _ehi200_returns(self):
        returns = pd.read_json(
            self._eurekahedge200_inputs["eurekahedge_returns"],
            orient="index",
        )
        if isinstance(returns, pd.DataFrame):
            return returns.squeeze()
        else:
            return pd.Series()

    @property
    def _eurekahedge_constituent_returns(self):
        returns = self._eurekahedge_inputs
        if returns is not None:
            returns = pd.read_json(returns["eurekahedge_constituent_returns"], orient="index")
            returns_columns = [ast.literal_eval(x) for x in returns.columns]
            returns_columns = pd.MultiIndex.from_tuples(
                returns_columns,
                names=["EurekahedgeBenchmark", "SourceInvestmentId"],
            )
            returns.columns = returns_columns
            return returns.droplevel(0, axis=1)
        else:
            return pd.Series()

    @property
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

    @property
    def _market_factor_inputs(self):
        if self._market_factor_inputs_cache is None:
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = "market_factor_returns_" + asofdate + ".json"
            self._market_factor_inputs_cache = self._download_inputs(
                location=self._underlying_data_location, file_path=file
            )
        return self._market_factor_inputs_cache

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
    def _entity_type(self):
        return "ARS PFUND"

    @property
    def _fund_id(self):
        return self._fund_dimn["InvestmentGroupId"]

    @property
    def _pub_investment_group_id(self):
        return self._fund_dimn["PubInvestmentGroupId"].squeeze()

    @property
    def _substrategy(self):
        return self._fund_dimn["SubStrategy"].squeeze()

    @property
    def _fund_rba(self):
        if self._all_rba is not None:
            return self._all_rba.copy()
        else:
            return pd.DataFrame()

    @property
    def _fund_rba_risk_decomp(self):
        decomp = self._all_rba_risk_decomp.copy()
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

    @property
    def _fund_rba_adj_r_squared(self):
        r2 = self._all_rba_adj_r_squared.copy()

        if r2.shape[0] == 0:
            return pd.DataFrame(index=["TTM", "3Y", "5Y"], columns=["AdjR2"])

        r2 = r2[r2["InvestmentGroupName"] == self._fund_name]
        r2 = r2[["1Y", "3Y", "5Y"]]
        r2.rename(columns={"1Y": "TTM"}, inplace=True)

        r2 = r2.T
        r2.columns = ["AdjR2"]
        r2 = r2.round(2)
        return r2

    @property
    def _fund_pba_publics(self):
        fund_index = self._all_pba_publics.columns.get_level_values(1) == self._fund_id.squeeze()
        if any(fund_index):
            fund_pba = self._all_pba_publics.iloc[:, fund_index]
            fund_pba.columns = fund_pba.columns.droplevel(1)
            return fund_pba
        else:
            return pd.DataFrame()

    @property
    def _fund_pba_privates(self):
        fund_index = self._all_pba_privates.columns.get_level_values(1) == self._fund_id.squeeze()
        if any(fund_index):
            fund_pba = self._all_pba_privates.iloc[:, fund_index]
            fund_pba.columns = fund_pba.columns.droplevel(1)
            return fund_pba
        else:
            return pd.DataFrame()

    @property
    def _primary_peer_group(self):
        group = self._fund_dimn["ReportingPeerGroup"].squeeze()
        if not isinstance(group, str):
            group = None
        return group

    @property
    def _secondary_peer_group(self):
        group = self._fund_dimn["StrategyPeerGroup"].squeeze()
        if not isinstance(group, str):
            group = None
        return group

    @property
    def _eurekahedge_benchmark(self):
        group = self._fund_dimn["EurekahedgeBenchmark"].squeeze()
        if not isinstance(group, str):
            group = None
        return group

    @property
    def _abs_return_benchmark(self):
        return self._fund_dimn["AbsoluteBenchmarkName"].squeeze()

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
    def _fle_scl(self):
        return self._fund_dimn["FleScl"].squeeze().round(2)

    @property
    def _risk_model_expected_return(self):
        return self._fund_dimn["RiskModelExpectedReturn"].squeeze().round(2)

    @property
    def _risk_model_expected_vol(self):
        return self._fund_dimn["RiskModelExpectedVol"].squeeze().round(2)

    @property
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

    def get_peer_ptile_2_heading(self):
        if self._secondary_peer_group is not None:
            group = self._secondary_peer_group.replace("GCM ", "")
            return pd.DataFrame({"peer_ptile_2_heading": [group]})
        else:
            return pd.DataFrame({"peer_ptile_2_heading": [""]})

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
                index=["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y"],
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

        # rounding to 2 so that Excess Return matches optically
        stats = [
            mtd_return,
            qtd_return,
            ytd_return,
            trailing_1y_return,
            trailing_3y_return,
            trailing_5y_return,
            trailing_10y_return,
        ]
        stats = [x.squeeze() for x in stats]
        summary = pd.DataFrame(
            {return_type: [round(x, 2) if isinstance(x, float) else " " for x in stats]},
            index=["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y"],
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

    def _calculate_periodic_percentile(self, constituent_returns, fund_periodic_return, period):
        periodic = self._analytics.compute_periodic_return(
            ror=constituent_returns,
            period=period,
            as_of_date=self._as_of_date,
            method="geometric",
        )
        periodics = pd.concat([pd.Series([fund_periodic_return.squeeze()]), periodic], axis=0)
        ptile = periodics.rank(pct=True)[0:1].squeeze().round(2) * 100
        return ptile

    def _calculate_trailing_percentile(self, constituent_returns, fund_periodic_return, trailing_months):
        returns = self._analytics.compute_trailing_return(
            ror=constituent_returns,
            window=trailing_months,
            as_of_date=self._as_of_date,
            method="geometric",
            annualize=True,
            periodicity=Periodicity.Monthly,
        )
        if isinstance(fund_periodic_return.squeeze(), float):
            returns = pd.concat(
                [pd.Series([fund_periodic_return.squeeze()]), returns],
                axis=0,
            )
            ptile = returns.rank(pct=True)[0:1].squeeze().round(2) * 100
        else:
            ptile = ""
        return ptile

    def _get_percentile_summary(self, fund_returns, constituent_returns, group_name):
        fund_returns = fund_returns.copy()
        constituent_returns = constituent_returns.copy()
        index = ["MTD", "QTD", "YTD", "TTM", "3Y", "5Y", "10Y"]

        if len(constituent_returns) > 0:
            mtd_ptile = self._calculate_periodic_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["MTD"],
                period=PeriodicROR.MTD,
            )
            qtd_ptile = self._calculate_periodic_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["QTD"],
                period=PeriodicROR.QTD,
            )
            ytd_ptile = self._calculate_periodic_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["YTD"],
                period=PeriodicROR.YTD,
            )
            trailing_1y_ptile = self._calculate_trailing_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["TTM"],
                trailing_months=12,
            )
            trailing_3y_ptile = self._calculate_trailing_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["3Y"],
                trailing_months=36,
            )
            trailing_5y_ptile = self._calculate_trailing_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["5Y"],
                trailing_months=60,
            )
            trailing_10y_ptile = self._calculate_trailing_percentile(
                constituent_returns=constituent_returns,
                fund_periodic_return=fund_returns.loc["10Y"],
                trailing_months=120,
            )

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
        else:
            summary = pd.DataFrame({group_name: [""] * len(index)}, index=index)

        return summary

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

        primary_peer_percentiles = self._get_percentile_summary(
            fund_returns=fund_returns,
            constituent_returns=self._primary_peer_constituent_returns,
            group_name="Peer1Ptile",
        )

        secondary_peer_percentiles = self._get_percentile_summary(
            fund_returns=fund_returns,
            constituent_returns=self._secondary_peer_constituent_returns,
            group_name="Peer2Ptile",
        )
        eurekahedge_percentiles = self._get_percentile_summary(
            fund_returns=fund_returns,
            constituent_returns=self._eurekahedge_constituent_returns,
            group_name="EH50Ptile",
        )

        ehi200_percentiles = self._get_percentile_summary(
            fund_returns=fund_returns,
            constituent_returns=self._ehi200_constituent_returns,
            group_name="EHI200Ptile",
        )

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
        summary = summary.merge(primary_peer_percentiles, left_index=True, right_index=True)
        summary = summary.merge(secondary_peer_percentiles, left_index=True, right_index=True)
        summary = summary.merge(eurekahedge_percentiles, left_index=True, right_index=True)
        summary = summary.merge(ehi200_percentiles, left_index=True, right_index=True)

        summary = summary.fillna("")
        return summary

    def build_constituent_count_summary(self):
        def _get_peers_with_returns_in_ttm(returns):
            return returns.notna()[-12:].any().sum()

        def _get_peers_with_current_month_return(returns):
            return returns.notna().sum(axis=1)[-1]

        def _summarize_counts(returns):
            if returns.shape[0] == 0:
                return [np.nan, np.nan]

            updated_constituents = _get_peers_with_current_month_return(returns)
            active_constituents = _get_peers_with_returns_in_ttm(returns)
            return [updated_constituents, active_constituents]

        primary = _summarize_counts(returns=self._primary_peer_constituent_returns)
        secondary = _summarize_counts(returns=self._secondary_peer_constituent_returns)
        eureka = _summarize_counts(returns=self._eurekahedge_constituent_returns)
        ehi200 = _summarize_counts(returns=self._ehi200_constituent_returns)

        summary = pd.DataFrame({'primary': primary,
                                'secondary': secondary,
                                'eureka': eureka,
                                'ehi200': ehi200})
        return summary

    @staticmethod
    def _get_exposure_summary(exposure):
        strategies = ['Equities', 'Credit', 'Macro']
        exposures = [x + 'Notional' for x in ['Long', 'Short', 'Gross', 'Net']]
        column_index = pd.MultiIndex.from_product([strategies, exposures], names=['ExposureStrategy', 'ExposureType'])
        periods = ["Latest", "3Y", "5Y", "10Y"]

        exposure_summary = exposure.drop(columns={'InvestmentGroupName', 'InvestmentGroupId', 'Date'})

        if all(exposure_summary.isna().all()):
            return pd.DataFrame(index=periods, columns=column_index)

        macro_strat = ~exposure_summary['ExposureStrategy'].isin(['Equities', 'Credit'])
        exposure_summary.loc[macro_strat, 'ExposureStrategy'] = 'Macro'
        exposure_summary = exposure_summary.groupby(['Period', 'ExposureStrategy']).sum()
        exposure_summary = exposure_summary.reset_index().set_index('Period')
        exposure_summary = exposure_summary.pivot(columns=['ExposureStrategy'])
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

        mtd = p_return_attribution(window=1, annualize=False).rename(columns={"CTR": "MTD"})
        qtd = p_return_attribution(window=qtd_window, annualize=False).rename(columns={"CTR": "QTD"})
        ytd = p_return_attribution(window=ytd_window, annualize=False).rename(columns={"CTR": "YTD"})
        ttm = p_return_attribution(window=12, annualize=False).rename(columns={"CTR": "TTM"})
        t3y = p_return_attribution(window=36, annualize=True).rename(columns={"CTR": "T3Y"})
        t5y = p_return_attribution(window=60, annualize=True).rename(columns={"CTR": "T5Y"})
        t10y = p_return_attribution(window=120, annualize=True).rename(columns={"CTR": "T10Y"})

        # TODO only fill na if some non na's
        summary = factor_group_index.merge(mtd, left_index=True, right_index=True, how="left")
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
        summary[~summary.isna().all(axis=1)] = summary[~summary.isna().all(axis=1)].fillna(0)
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
        summary = factor_group_index.merge(mtd_publics, left_index=True, right_index=True, how="left")
        summary = summary.merge(mtd_privates, left_index=True, right_index=True, how="left")
        summary = summary.merge(qtd_publics, left_index=True, right_index=True, how="left")
        summary = summary.merge(qtd_privates, left_index=True, right_index=True, how="left")
        summary = summary.merge(ytd_publics, left_index=True, right_index=True, how="left")
        summary = summary.merge(ytd_privates, left_index=True, right_index=True, how="left")
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
        summary[~summary.isna().all(axis=1)] = summary[~summary.isna().all(axis=1)].fillna(0)
        return summary

    def build_exposure_summary(self):
        summary = self._get_exposure_summary(self._exposure)

        # nullify exposure where prior to track
        length_of_track = self._fund_returns.shape[0]

        if length_of_track < 12:
            summary.loc['1Y'] = np.nan

        if length_of_track < 36:
            summary.loc['3Y'] = np.nan

        if length_of_track < 60:
            summary.loc['5Y'] = np.nan

        if length_of_track < 120:
            summary.loc['10Y'] = np.nan

        return summary

    def build_rba_summary(self):
        if self._fund_rba.shape[0] > 0:
            # fund_returns = self._get_return_summary(returns=self._fund_returns, return_type='Fund')
            # fund_returns.rename(columns={'Fund': 'Total'}, inplace=True)
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
        return self._analytics.compute_trailing_sharpe_ratio(
            ror=returns,
            rf_ror=self._rf_return,
            window=trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            include_history=True,
        )

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

    def _get_fund_trailing_vol_summary(self):
        returns = self._fund_returns.copy()
        trailing_1y_vol = self._get_trailing_vol(returns=returns, trailing_months=12)
        trailing_3y_vol = self._get_trailing_vol(returns=returns, trailing_months=36)
        trailing_5y_vol = self._get_trailing_vol(returns=returns, trailing_months=60)
        rolling_1_vol = self._get_rolling_vol(returns=returns, trailing_months=12)
        trailing_5y_median_vol = self._summarize_rolling_median(rolling_1_vol, trailing_months=60)

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
        trailing_1y_beta = self._get_trailing_beta(returns=returns, trailing_months=12)
        trailing_3y_beta = self._get_trailing_beta(returns=returns, trailing_months=36)
        trailing_5y_beta = self._get_trailing_beta(returns=returns, trailing_months=60)

        returns_10y = returns[returns.index >= self._sp500_return.index.min()]
        if returns_10y.shape[0] >= 12:
            rolling_1y_beta = self._get_rolling_beta(returns=returns_10y, trailing_months=12)
            trailing_5y_median_beta = self._summarize_rolling_median(rolling_1y_beta, trailing_months=60)
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
        trailing_1y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=12)
        trailing_3y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=36)
        trailing_5y_sharpe = self._get_trailing_sharpe(returns=returns, trailing_months=60)
        rolling_1y_sharpe = self._get_rolling_sharpe_ratio(returns=returns, trailing_months=12)
        trailing_5y_median_sharpe = self._summarize_rolling_median(rolling_1y_sharpe, trailing_months=60)

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
        trailing_1y_batting_avg = self._get_trailing_batting_avg(returns=returns, trailing_months=12)
        trailing_3y_batting_avg = self._get_trailing_batting_avg(returns=returns, trailing_months=36)
        trailing_5y_batting_avg = self._get_trailing_batting_avg(returns=returns, trailing_months=60)
        rolling_1y_batting_avg = self._get_rolling_batting_avg(returns=returns, trailing_months=12)
        trailing_5y_median_batting_avg = self._summarize_rolling_median(rolling_1y_batting_avg, trailing_months=60)

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
        trailing_1y_win_loss = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_3y_win_loss = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=36)
        trailing_5y_win_loss = self._get_trailing_win_loss_ratio(returns=returns, trailing_months=60)
        rolling_1y_win_loss = self._get_rolling_win_loss_ratio(returns=returns, trailing_months=12)
        trailing_5y_median_win_loss = self._summarize_rolling_median(rolling_1y_win_loss, trailing_months=60)

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
        rolling_12m_returns = self._get_rolling_return(returns=returns, trailing_months=12)
        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=12)
        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=36)
        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_returns, trailing_months=60)

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
        rolling_12m_sharpes = self._get_rolling_sharpe_ratio(returns=returns, trailing_months=12)
        rolling_1y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=12)
        rolling_3y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=36)
        rolling_5y_summary = self._summarize_rolling_data(rolling_data=rolling_12m_sharpes, trailing_months=60)

        summary = pd.concat(
            [
                rolling_1y_summary.T,
                rolling_3y_summary.T,
                rolling_5y_summary.T,
            ]
        )
        summary.index = ["TTM", "3Y", "5Y"]

        return summary

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
            asofdate = self._as_of_date.strftime("%Y-%m-%d")
            file = self._primary_peer_group.replace("/", "") + "_peer_" + asofdate + ".json"
            summary = self._download_inputs(location=self._summary_data_location, file_path=file)
            summary = pd.read_json(
                summary["performance_stability_peer_summary"],
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

    def build_risk_model_expectations_summary(self):
        summary = pd.DataFrame(
            {
                "Expectations": [
                    self._risk_model_expected_return.copy(),
                    self._risk_model_expected_vol.copy(),
                ]
            },
            index=["ExpectedReturn", "ExpectedVolatility"],
        )
        return summary

    def build_monthly_performance_summary(self):
        end_year = self._as_of_date.year
        years = [x for x in range(end_year - 52, end_year + 1)]
        months = [x for x in range(1, 13)]

        if self._fund_returns.shape[0] == 0:
            return pd.DataFrame(columns=['Month', 'Year'] + months + ['YTD'], index=years)

        returns = self._fund_returns.copy()
        returns['Month'] = returns.index.month
        returns['Year'] = returns.index.year

        # pivot long to wide
        monthly_returns = returns.pivot(index=['Year'], columns=['Month'])
        monthly_returns = monthly_returns.reindex(years, axis=0)
        monthly_returns.columns = monthly_returns.columns.droplevel(0)
        monthly_returns = monthly_returns.reindex(months, axis=1)
        monthly_returns = monthly_returns.sort_values('Year', ascending=False)

        # append ytd returns
        ytd_returns = monthly_returns.apply(lambda x: np.prod(1 + x) - 1, axis=1)
        ytd_index = ~monthly_returns.isna().all(axis=1)
        monthly_returns['YTD'] = None
        monthly_returns.loc[ytd_index, 'YTD'] = ytd_returns[ytd_index]

        monthly_returns = 100 * monthly_returns.astype(float).round(3)
        monthly_returns = monthly_returns.reset_index()

        monthly_returns.loc[~ytd_index.values, 'Year'] = None

        return monthly_returns

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

        return_summary = self.build_benchmark_summary()
        constituent_count_summary = self.build_constituent_count_summary()
        absolute_return_benchmark = self.get_absolute_return_benchmark()
        peer_group_heading = self.get_peer_group_heading()
        eurekahedge_benchmark_heading = self.get_eurekahedge_benchmark_heading()
        peer_ptile_1_heading = self.get_peer_ptile_1_heading()
        peer_ptile_2_heading = self.get_peer_ptile_2_heading()

        rba_summary = self.build_rba_summary()
        adj_r2 = self.get_adj_r2()
        pba_summary = self.build_pba_summary()
        pba_mtd = pba_summary.loc[["MTD - Publics", "MTD - Privates"]]
        pba_qtd = pba_summary.loc[["QTD - Publics", "QTD - Privates"]]
        pba_ytd = pba_summary.loc[["YTD - Publics", "YTD - Privates"]]

        performance_stability_fund_summary = self.build_performance_stability_fund_summary()
        performance_stability_peer_summary = self.build_performance_stability_peer_summary()

        shortfall_summary = self.build_shortfall_summary()
        risk_model_expectations = self.build_risk_model_expectations_summary()

        exposure_summary = self.build_exposure_summary()
        latest_exposure_heading = self.get_latest_exposure_heading()

        monthly_performance_summary = self.build_monthly_performance_summary()

        logging.info('Report summary data generated for: ' + self._fund_name)

        input_data = {
            "header_info": header_info,
            "benchmark_summary": return_summary,
            "constituent_count_summary": constituent_count_summary,
            "absolute_return_benchmark": absolute_return_benchmark,
            "peer_group_heading": peer_group_heading,
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading,
            "peer_ptile_1_heading": peer_ptile_1_heading,
            "peer_ptile_2_heading": peer_ptile_2_heading,
            "performance_stability_fund_summary": performance_stability_fund_summary,
            "performance_stability_peer_summary": performance_stability_peer_summary,
            "rba_summary": rba_summary,
            "adj_r2": adj_r2,
            "pba_mtd": pba_mtd,
            "pba_qtd": pba_qtd,
            "pba_ytd": pba_ytd,
            "shortfall_summary": shortfall_summary,
            "risk_model_expectations": risk_model_expectations,
            "exposure_summary": exposure_summary,
            "latest_exposure_heading": latest_exposure_heading,
            "monthly_performance_summary": monthly_performance_summary,
        }

        input_data_json = {
            "header_info": header_info.to_json(orient='index'),
            "benchmark_summary": return_summary.to_json(orient='index'),
            "constituent_count_summary": constituent_count_summary.to_json(orient='index'),
            "absolute_return_benchmark": absolute_return_benchmark.to_json(orient='index'),
            "peer_group_heading": peer_group_heading.to_json(orient='index'),
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading.to_json(orient='index'),
            "peer_ptile_1_heading": peer_ptile_1_heading.to_json(orient='index'),
            "peer_ptile_2_heading": peer_ptile_2_heading.to_json(orient='index'),
            "performance_stability_fund_summary": performance_stability_fund_summary.to_json(orient='index'),
            "performance_stability_peer_summary": performance_stability_peer_summary.to_json(orient='index'),
            "rba_summary": rba_summary.to_json(orient='index'),
            "adj_r2": adj_r2.to_json(orient='index'),
            "pba_mtd": pba_mtd.to_json(orient='index'),
            "pba_qtd": pba_qtd.to_json(orient='index'),
            "pba_ytd": pba_ytd.to_json(orient='index'),
            "shortfall_summary": shortfall_summary.to_json(orient='index'),
            "risk_model_expectations": risk_model_expectations.to_json(orient='index'),
            "exposure_summary": exposure_summary.to_json(orient='index'),
            "latest_exposure_heading": latest_exposure_heading.to_json(orient='index'),
            "monthly_performance_summary": monthly_performance_summary.to_json(orient='index')
        }

        data_to_write = json.dumps(input_data_json)
        asofdate = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            self._fund_name.replace("/", "") + "_fund_" + asofdate + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

        logging.info("JSON stored to DataLake for: " + self._fund_name)

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="PFUND_PerformanceQuality_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name=self._fund_name,
                entity_display_name=self._fund_name.replace("/", ""),
                entity_ids=[self._pub_investment_group_id.item()],
                entity_source=DaoSource.PubDwh,
                report_name="Performance Quality",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                aggregate_intervals=AggregateInterval.MTD,
                output_dir="cleansed/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake,
            )

        logging.info("Excel stored to DataLake for: " + self._fund_name)

    def run(self, **kwargs):
        self.generate_performance_quality_report()
        return True
