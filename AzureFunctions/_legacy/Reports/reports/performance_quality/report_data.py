import json
from functools import cached_property

import pandas as pd
import datetime as dt
import os
from gcm.inv.scenario import Scenario
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from pandas._libs.tslibs.offsets import relativedelta
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.dataprovider.peer_group import PeerGroup
from gcm.inv.utils.date import DatePeriod


class PerformanceQualityReportData(ReportingRunnerBase):
    def __init__(self, start_date, end_date, investment_group_ids=None):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._start_date = start_date
        self._end_date = end_date
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._analytics = Analytics()
        self._inv_group_ids = investment_group_ids
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/performancequality"
        self._benchmarks = StrategyBenchmark()
        self._peers = PeerGroup()
        self._start_1y = self._end_date - relativedelta(years=1)
        self._start_3y = self._end_date - relativedelta(years=3)
        self._start_5y = self._end_date - relativedelta(years=5)
        self._start_10y = self._end_date - relativedelta(years=10)

    def _filter_fund_set(self):
        if self._inv_group_ids is None:
            include_filters = dict(status=["EMM", "HPMM"])
            exclude_filters = dict(strategy=["Other", "Aggregated Prior Period Adjustment"])
            exclude_gcm_portfolios = True
        else:
            include_filters = None
            exclude_filters = None
            exclude_gcm_portfolios = False

        fund_dimn = InvestmentGroup(investment_group_ids=self._inv_group_ids).get_dimensions(
            exclude_gcm_portfolios=exclude_gcm_portfolios,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        return fund_dimn

    @staticmethod
    def _subset_fund_dimn(fund_dimn):
        fund_dimn_columns = [
            "InvestmentGroupId",
            "PubInvestmentGroupId",
            "InvestmentGroupName",
            "AbsoluteBenchmarkId",
            "AbsoluteBenchmarkName",
            "EurekahedgeBenchmark",
            "InceptionDate",
            "InvestmentStatus",
            "ReportingPeerGroup",
            "StrategyPeerGroup",
            "Strategy",
            "SubStrategy",
            "FleScl",
        ]

        fund_dimn = fund_dimn.reindex(columns=fund_dimn_columns, fill_value=None)
        return fund_dimn

    @cached_property
    def _fund_dimn(self):
        return self._subset_fund_dimn(fund_dimn=self._filter_fund_set())

    @cached_property
    def _inv_group(self) -> InvestmentGroup:
        return InvestmentGroup(investment_group_ids=self._fund_dimn['InvestmentGroupId'])

    @cached_property
    def _report_fund_returns(self):
        fund_monthly_returns = self._inv_group.get_monthly_returns(
            start_date=dt.date(1970, 1, 1),
            end_date=self._end_date,
            wide=True,
            priority_waterfall=None,
        )
        #fund_monthly_returns=fund_monthly_returns.sort_index()
        return fund_monthly_returns

    @cached_property
    def _abs_bmrk_returns(self):
        return self._inv_group.get_absolute_benchmark_returns(start_date=self._start_date,
                                                              end_date=self._end_date)

    @cached_property
    def _abs_bmrk_betas(self):
        return self._inv_group.get_absolute_benchmark_betas()

    @cached_property
    def _eurekahedge_bmrk_names(self):
        bmrks = self._fund_dimn["EurekahedgeBenchmark"]
        eh_benchmark_names = bmrks.unique().tolist() + ["Eurekahedge Institutional 200"]
        eh_benchmark_names = [x for x in eh_benchmark_names if (str(x) != "nan") and (x is not None)]
        return eh_benchmark_names

    @cached_property
    def _eurekahedge_returns(self):
        eurekahedge_returns = self._benchmarks.get_eurekahedge_returns(start_date=self._start_date,
                                                                       end_date=self._end_date,
                                                                       benchmarks_names=self._eurekahedge_bmrk_names)
        return eurekahedge_returns

    @cached_property
    def _ehi_constituent_returns(self):
        return self._benchmarks.get_eurekahedge_constituent_returns(
            start_date=self._start_date, end_date=self._end_date, benchmarks_names=self._eurekahedge_bmrk_names
        )

    @cached_property
    def _peer_groups(self):
        peers = self._fund_dimn["ReportingPeerGroup"].tolist() + self._fund_dimn["StrategyPeerGroup"].tolist()
        peers = list(set(peers))
        peers = [x for x in peers if (str(x) != "nan") and (x is not None)]
        return peers

    @cached_property
    def _peer_bmrk_returns(self):
        return self._benchmarks.get_altsoft_peer_returns(start_date=self._start_date,
                                                         end_date=self._end_date,
                                                         peer_names=self._peer_groups)

    @cached_property
    def _peer_constituent_returns(self):
        # y=self._peers.get_constituent_returns(
        #     start_date=dt.date(2000, 1, 1), end_date=self._end_date, peer_groups=self._peer_groups
        # )
        return self._peers.get_constituent_returns(
            start_date=dt.date(2000, 1, 1), end_date=self._end_date, peer_groups=self._peer_groups
        )

    def _get_exposures(self):
        exposure_latest = self._inv_group.get_latest_exposure(as_of_date=self._as_of_date)
        exposure_3y = self._inv_group.get_average_exposure(start_date=self._start_3y, end_date=self._end_date)
        exposure_5y = self._inv_group.get_average_exposure(start_date=self._start_5y, end_date=self._end_date)
        exposure_10y = self._inv_group.get_average_exposure(start_date=self._start_10y, end_date=self._end_date)
        return exposure_latest, exposure_3y, exposure_5y, exposure_10y

    @cached_property
    def _get_net_exposures(self):
        fund_monthly_exposures = self._inv_group.get_monthly_pub_exposure(
            start_date=dt.date(1970, 1, 1),
            end_date=self._end_date
        )
        fund_exp_net_df = fund_monthly_exposures[[
            'InvestmentGroupName', 'Date', 'ExposureStrategy', 'NetNotional']]
        fund_exp_net_df = fund_monthly_exposures[fund_monthly_exposures[
            'ExposureStrategy'].isin(['Credit', 'Equities'])]
        fund_exp_net_df = fund_exp_net_df.groupby([
            'InvestmentGroupName', 'Date']).agg({'NetNotional': 'sum'}).reset_index()
        fund_exp_net_timeseries = fund_exp_net_df.sort_values(by=[
            'InvestmentGroupName', 'Date'])
        i = pd.date_range(dt.date(1970, 1, 1), self._end_date, freq='M', name='Date')
        fund_exp_net_timeseries = fund_exp_net_timeseries.set_index(
            'Date').groupby('InvestmentGroupName', group_keys=False)\
            .apply(lambda s: s.reindex(i).ffill()).reset_index()

        fund_exp_net_timeseries = fund_exp_net_timeseries.pivot_table(
            index='Date', columns='InvestmentGroupName', values='NetNotional', dropna=False)
        fund_exp_net_timeseries.fillna(method='ffill', inplace=True)

        return fund_exp_net_timeseries

    @cached_property
    def _get_gross_exposures(self):
        fund_monthly_exposures = self._inv_group.get_monthly_pub_exposure(
            start_date=dt.date(1970, 1, 1),
            end_date=self._end_date
        )
        fund_monthly_exposures = fund_monthly_exposures[[
            'InvestmentGroupName', 'Date', 'GrossNotional']]
        fund_monthly_exposures = fund_monthly_exposures.groupby(
            ['InvestmentGroupName', 'Date']).agg({'GrossNotional': 'sum'}).reset_index()
        fund_monthly_gross_exposures = fund_monthly_exposures.sort_values(
            by=['InvestmentGroupName', 'Date'])
        i = pd.date_range(dt.date(1970, 1, 1), self._end_date, freq='M', name='Date')
        fund_monthly_gross_exposures = fund_monthly_gross_exposures.set_index('Date').groupby(
            'InvestmentGroupName', group_keys=False)\
            .apply(lambda s: s.reindex(i).ffill()).reset_index()

        fund_monthly_exposures_timeseries = fund_monthly_gross_exposures.pivot_table(
            index='Date', columns='InvestmentGroupName', values='GrossNotional', dropna=False)

        fund_monthly_exposures_timeseries.fillna(method='ffill', inplace=True)
        fund_monthly_exposures_timeseries = fund_monthly_exposures_timeseries.reset_index()

        fund_monthly_exposures_timeseries['Date'] = pd.to_datetime(
            fund_monthly_exposures_timeseries['Date'])
        fund_monthly_exposures_timeseries.set_index('Date', inplace=True)
        return fund_monthly_exposures_timeseries

    def _get_rf_and_spx(self):
        market_factor_returns = Factor(tickers=["I00078US Index", "SPXT Index"]).get_returns(
            start_date=self._start_10y,
            end_date=self._end_date,
            fill_na=True,
        )
        return market_factor_returns

    @cached_property
    def _peer_arb_mapping(self):
        # TODO replace with peer_group_arb_mapping used in InvestmentsModels
        peer_arb_mapping = pd.read_csv(os.path.dirname(__file__) + "/peer_group_to_arb_mapping.csv")
        peer_arb_mapping.loc[peer_arb_mapping['ReportingPeerGroup'] == 'GCM Macro', 'Ticker'] = 'MOVE Index'
        return peer_arb_mapping

    def _get_peer_benchmark_returns(self):
        peer_benchmarks = self._peer_arb_mapping['Ticker'].unique().tolist()
        peer_benchmark_returns = Factor(tickers=peer_benchmarks).get_returns(
            start_date=dt.date(2000, 1, 1),
            end_date=self._end_date,
            fill_na=True,
        )
        return peer_benchmark_returns

    @cached_property
    def _move_index_levels(self):
        move_levels = Factor(tickers=["MOVE Index"]).get_dimensions(DatePeriod(start_date=dt.date(2000, 1, 1),
                                                                               end_date=self._end_date))
        move_levels = move_levels.pivot_table(index="Date", columns="Ticker", values="PxLast")
        return move_levels

    def _get_rba_attribution(self):
        return self._inv_group.get_rba_return_decomposition_by_date(
            start_date=self._start_date,
            end_date=self._end_date,
            factor_filter=[
                "SYSTEMATIC",
                "REGION",
                "INDUSTRY",
                "LS_EQUITY",
                "LS_CREDIT",
                "MACRO",
                "NON_FACTOR_SECURITY_SELECTION",
                "NON_FACTOR_OUTLIER_EFFECTS",
            ],
            frequency="M",
            window=36,
        )

    def _get_rba_risk(self):
        rba_risk_1y = self._inv_group.get_average_risk_decomp_by_group(
            start_date=self._start_1y,
            end_date=self._end_date,
            group_type="FactorGroup1",
            frequency="M",
            window=36,
            wide=False,
        )
        rba_risk_1y.rename(columns={"AvgRiskContrib": "1Y"}, inplace=True)

        rba_risk_3y = self._inv_group.get_average_risk_decomp_by_group(
            start_date=self._start_3y,
            end_date=self._end_date,
            group_type="FactorGroup1",
            frequency="M",
            window=36,
            wide=False,
        )
        rba_risk_3y.rename(columns={"AvgRiskContrib": "3Y"}, inplace=True)

        rba_risk_5y = self._inv_group.get_average_risk_decomp_by_group(
            start_date=self._start_5y,
            end_date=self._end_date,
            group_type="FactorGroup1",
            frequency="M",
            window=36,
            wide=False,
        )
        rba_risk_5y.rename(columns={"AvgRiskContrib": "5Y"}, inplace=True)

        rba_risk_decomp = rba_risk_1y.merge(rba_risk_3y, how="outer").merge(rba_risk_5y, how="outer")
        rba_risk_decomp = rba_risk_decomp.fillna(0)
        return rba_risk_decomp

    def _get_rba_r2(self):
        rba_r2_1y = self._inv_group.get_average_adj_r2(
            start_date=self._start_1y,
            end_date=self._end_date,
            frequency="M",
            window=36,
        )
        rba_r2_1y.rename(columns={"AvgAdjR2": "1Y"}, inplace=True)

        rba_r2_3y = self._inv_group.get_average_adj_r2(
            start_date=self._start_3y,
            end_date=self._end_date,
            frequency="M",
            window=36,
        )
        rba_r2_3y.rename(columns={"AvgAdjR2": "3Y"}, inplace=True)

        rba_r2_5y = self._inv_group.get_average_adj_r2(
            start_date=self._start_5y,
            end_date=self._end_date,
            frequency="M",
            window=36,
        )
        rba_r2_5y.rename(columns={"AvgAdjR2": "5Y"}, inplace=True)

        rba_adj_r_squared = rba_r2_1y.merge(rba_r2_3y, how="outer").merge(rba_r2_5y, how="outer")
        return rba_adj_r_squared

    def _get_rba_statistics(self):
        attrib = self._get_rba_attribution()
        risk_decomp = self._get_rba_risk()
        adj_r_squared = self._get_rba_r2()
        return attrib, risk_decomp, adj_r_squared

    def _get_pba_attribution(self):
        publics = self._inv_group.get_pba_ts_by_group(
            start_date=self._start_date,
            end_date=self._end_date,
            group_type="FactorGroup",
            frequency="M",
            public_or_private="Public",
        )

        privates = self._inv_group.get_pba_ts_by_group(
            start_date=self._start_date,
            end_date=self._end_date,
            group_type="FactorGroup",
            frequency="M",
            public_or_private="Private",
        )
        return publics, privates

    @cached_property
    def _fund_expectations(self):
        return self._inv_group.get_fund_expectations()

    @cached_property
    def _fund_fl_expected_returns(self):
        return self._inv_group.get_fund_fl_expected_returns()

    @cached_property
    def _fund_distributions(self):
        # TODO - get 1Y lagged distributions to compare for forward returns
        return self._inv_group.get_simulated_fund_returns()

    def get_performance_quality_report_inputs(self):
        exposure_latest, exposure_3y, exposure_5y, exposure_10y = self._get_exposures()
        #fund_exp_net_timeseries, fund_monthly_exposures_timeseries = self._get_timeseries_exposures
        market_factor_returns = self._get_rf_and_spx()
        peer_benchmark_returns = self._get_peer_benchmark_returns()

        rba, rba_risk_decomp, rba_adj_r_squared = self._get_rba_statistics()
        pba_publics, pba_privates = self._get_pba_attribution()

        report_inputs = dict()
        report_inputs["fund_inputs"] = dict()
        report_inputs["peer_inputs"] = dict()
        report_inputs["eurekahedge_inputs"] = dict()
        report_inputs["market_factor_returns"] = dict()
        for fund_id in self._fund_dimn["InvestmentGroupId"]:
            fund_inputs = dict()
            dimn = self._fund_dimn[self._fund_dimn["InvestmentGroupId"] == fund_id]
            fund_inputs["fund_dimn"] = dimn.to_json(orient="index")
            name = dimn["InvestmentGroupName"].squeeze()

            # expectations = self._fund_expectations[self._fund_expectations["InvestmentGroupId"] == fund_id]
            # fund_inputs["expectations"] = expectations.to_json(orient="index")

            # fl_expected_returns=self._fund_fl_expected_returns[self._fund_fl_expected_returns["InvestmentGroupId"] == fund_id]
            # fund_inputs["fl_expected_returns"] = fl_expected_returns.to_json(orient="index")

            returns = self._report_fund_returns.loc[:, self._report_fund_returns.columns.isin([name])].dropna()
            fund_inputs["fund_returns"] = returns.to_json(orient="index")

            gross_exposures = self._get_gross_exposures.loc[:, self._get_gross_exposures.columns.isin([name])].dropna()
            fund_inputs["gross_exposures"] = gross_exposures.to_json(orient="index", date_format='iso')

            net_exposures = self._get_net_exposures.loc[:, self._get_net_exposures.columns.isin([name])].dropna()
            fund_inputs["net_exposures"] = net_exposures.to_json(orient="index")

            abs_returns = self._abs_bmrk_returns.loc[:, self._abs_bmrk_returns.columns.isin([fund_id])]
            fund_inputs["abs_bmrk_returns"] = abs_returns.to_json(orient="index")

            exp_latest = exposure_latest[exposure_latest["InvestmentGroupId"] == fund_id]
            fund_inputs["exposure_latest"] = exp_latest.to_json(orient="index")

            exp_3y = exposure_3y[exposure_3y["InvestmentGroupId"] == fund_id]
            fund_inputs["exposure_3y"] = exp_3y.to_json(orient="index")

            exp_5y = exposure_5y[exposure_5y["InvestmentGroupId"] == fund_id]
            fund_inputs["exposure_5y"] = exp_5y.to_json(orient="index")

            exp_10y = exposure_10y[exposure_10y["InvestmentGroupId"] == fund_id]
            fund_inputs["exposure_10y"] = exp_10y.to_json(orient="index")

            if rba.shape[0] > 0:
                fund_rba = rba.iloc[:, rba.columns.get_level_values(1) == fund_id]
                fund_rba.columns = fund_rba.columns.droplevel(0).droplevel(0)
            else:
                fund_rba = rba.copy()
            fund_inputs["rba"] = fund_rba.to_json(orient="index")

            decomp = rba_risk_decomp[rba_risk_decomp["InvestmentGroupId"] == fund_id]
            fund_inputs["rba_risk_decomp"] = decomp.to_json(orient="index")

            r2 = rba_adj_r_squared[rba_adj_r_squared["InvestmentGroupId"] == fund_id]
            fund_inputs["rba_adj_r_squared"] = r2.to_json(orient="index")

            publics_index = pba_publics.columns.get_level_values(1) == fund_id
            fund_inputs["pba_publics"] = pba_publics.iloc[:, publics_index].to_json(orient="index")

            privates_index = pba_privates.columns.get_level_values(1) == fund_id
            fund_inputs["pba_privates"] = pba_privates.iloc[:, privates_index].to_json(orient="index")

            fund_arb_id = dimn['AbsoluteBenchmarkId'].squeeze()
            if fund_arb_id in self._abs_bmrk_betas.columns.values:
                fund_arb_betas = self._abs_bmrk_betas[fund_arb_id]
                fund_arb_betas = fund_arb_betas[fund_arb_betas != 0]
            else:
                fund_arb_betas = pd.Series(dtype=object)
            fund_inputs["abs_bmrk_betas"] = fund_arb_betas.to_json(orient="index")

            expectations = self._fund_expectations[self._fund_expectations["InvestmentGroupId"] == fund_id]
            fund_inputs["expectations"] = expectations.to_json(orient="index")

            fl_expected_returns = self._fund_fl_expected_returns[self._fund_fl_expected_returns["InvestmentGroupId"] == fund_id]
            fund_inputs["fl_expected_returns"] = fl_expected_returns.to_json(orient="index")

            if fund_id in self._fund_distributions.columns:
                distributions = self._fund_distributions[fund_id]
            else:
                distributions = pd.DataFrame()
            fund_inputs["distributions"] = distributions.to_json(orient="index")

            report_inputs["fund_inputs"][name] = fund_inputs

        for peer in self._peer_bmrk_returns.columns:
            peer_inputs = dict()
            peer_inputs["gcm_peer_returns"] = self._peer_bmrk_returns[peer].to_json(orient="index")
            peer_index = self._peer_constituent_returns.columns.get_level_values(0) == peer
            constituents = self._peer_constituent_returns.iloc[:, peer_index]
            peer_inputs["gcm_peer_constituent_returns"] = constituents.to_json(orient="index")

            peer_arb = self._peer_arb_mapping[self._peer_arb_mapping['ReportingPeerGroup'] == peer]
            bmrk = 'GDDUWI Index' if peer_arb.shape[0] == 0 else peer_arb['Ticker'].squeeze()
            peer_bmrk_returns = self._move_index_levels if peer == "GCM Macro" else peer_benchmark_returns[[bmrk]]

            peer_inputs["peer_group_abs_return_bmrk_returns"] = peer_bmrk_returns.to_json(orient="index")

            report_inputs["peer_inputs"][peer] = peer_inputs

        for eh_name in self._eurekahedge_returns.columns:
            eh_inputs = dict()
            eh_inputs["eurekahedge_returns"] = self._eurekahedge_returns[eh_name].to_json(orient="index")
            eh_index = self._ehi_constituent_returns.columns.get_level_values(0) == eh_name
            eh_constituents = self._ehi_constituent_returns.iloc[:, eh_index]
            eh_inputs["eurekahedge_constituent_returns"] = eh_constituents.to_json(orient="index")

            report_inputs["eurekahedge_inputs"][eh_name] = eh_inputs

        report_inputs["market_factor_returns"] = market_factor_returns.to_json(orient="index")
        report_inputs["filtered_peers"] = self._peer_groups

        return report_inputs

    def generate_inputs_and_write_to_datalake(self) -> dict:
        inputs = self.get_performance_quality_report_inputs()
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")

        fund_names = sorted(list(inputs["fund_inputs"].keys()))
        for fund in fund_names:
            fund_input = json.dumps(inputs["fund_inputs"][fund])
            write_params = AzureDataLakeDao.create_get_data_params(
                self._underlying_data_location,
                fund.replace("/", "") + "_fund_inputs_" + as_of_date + ".json",
                retry=False,
            )
            self._runner.execute(
                params=write_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, fund_input),
            )

        peer_names = list(set(inputs["filtered_peers"]))
        peer_names = [x for x in peer_names if pd.isnull(x) is False]
        peer_names = sorted(peer_names)
        for peer in peer_names:
            peer_input = json.dumps(inputs["peer_inputs"][peer])
            write_params = AzureDataLakeDao.create_get_data_params(
                self._underlying_data_location,
                peer.replace("/", "") + "_peer_inputs_" + as_of_date + ".json",
                retry=False,
            )
            self._runner.execute(
                params=write_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, peer_input),
            )

        eh_names = sorted(list(inputs["eurekahedge_inputs"].keys()))
        for eh in eh_names:
            eh_input = json.dumps(inputs["eurekahedge_inputs"][eh])
            write_params = AzureDataLakeDao.create_get_data_params(
                self._underlying_data_location,
                eh.replace("/", "") + "_eurekahedge_inputs_" + as_of_date + ".json",
                retry=False,
            )
            self._runner.execute(
                params=write_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, eh_input),
            )

        market_factor_returns = json.dumps(inputs["market_factor_returns"])
        write_params = AzureDataLakeDao.create_get_data_params(
            self._underlying_data_location,
            "market_factor_returns_" + as_of_date + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, market_factor_returns),
        )

        funds = dict({"fund_names": fund_names, "peer_groups": peer_names})
        funds = json.dumps(funds)
        return funds

    def run(self, **kwargs):
        return self.generate_inputs_and_write_to_datalake()


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

    as_of_date = dt.date(2022, 10, 31)
    with Scenario(dao=runner, as_of_date=as_of_date).context():
        ids = [20016, 23441, 75614, 28015]  # prd
        # ids = [19224, 23319, 74984]  # dev
        start_date = as_of_date - relativedelta(years=10)
        report_data = PerformanceQualityReportData(start_date=start_date, end_date=as_of_date,
                                                   investment_group_ids=ids)
        report_data.execute()
