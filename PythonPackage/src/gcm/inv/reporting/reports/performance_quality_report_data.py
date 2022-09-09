import json
import pandas as pd
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from pandas._libs.tslibs.offsets import relativedelta
from .reporting_runner_base import ReportingRunnerBase
from gcm.inv.quantlib.timeseries.analytics import Analytics


class PerformanceQualityReportData(ReportingRunnerBase):

    def __init__(self, start_date, end_date, investment_group_ids=None):
        super().__init__(runner=Scenario.get_attribute('runner'))
        self._start_date = start_date
        self._end_date = end_date
        self._as_of_date = Scenario.get_attribute('as_of_date')
        self._analytics = Analytics()
        self._entity_type = 'ARS PFUND'
        self._status = 'EMM'
        self._inv_group_ids = investment_group_ids
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/performancequality"

    def get_performance_quality_report_inputs(self):
        # pre-filtering to EMMs to avoid performance issues. refactor later to occur behind the scenes in data provider
        if self._inv_group_ids is None:
            include_filters = dict(status=[self._status])
            exclude_filters = dict(strategy=['Other', 'Aggregated Prior Period Adjustment'])
            exclude_gcm_portfolios = True
        else:
            include_filters = None
            exclude_filters = None
            exclude_gcm_portfolios = False

        inv_group = InvestmentGroup(investment_group_ids=self._inv_group_ids)
        fund_dimn = inv_group.get_dimensions(exclude_gcm_portfolios=exclude_gcm_portfolios,
                                             include_filters=include_filters,
                                             exclude_filters=exclude_filters)

        fund_dimn_columns = ['InvestmentGroupId', 'PubInvestmentGroupId', 'InvestmentGroupName',
                             'AbsoluteBenchmarkId', 'AbsoluteBenchmarkName', 'EurekahedgeBenchmark',
                             'InceptionDate', 'InvestmentStatus', 'ReportingPeerGroup', 'StrategyPeerGroup',
                             'Strategy', 'SubStrategy', 'FleScl', 'RiskModelExpectedReturn', 'RiskModelExpectedVol']

        fund_dimn = fund_dimn.reindex(columns=fund_dimn_columns, fill_value=None)

        filter_ids = fund_dimn['InvestmentGroupId']
        inv_group = InvestmentGroup(investment_group_ids=filter_ids)
        fund_monthly_returns = inv_group.get_monthly_returns(start_date=self._start_date,
                                                             end_date=self._end_date,
                                                             wide=True,
                                                             priority_waterfall=None)

        abs_bmrk_returns = inv_group.get_absolute_benchmark_returns(start_date=self._start_date,
                                                                    end_date=self._end_date)

        benchmarks = StrategyBenchmark()
        eurekahedge_returns = benchmarks.get_eurekahedge_returns(start_date=self._start_date,
                                                                 end_date=self._end_date)

        gcm_peer_returns = benchmarks.get_altsoft_peer_returns(start_date=self._start_date, end_date=self._end_date)

        gcm_peer_constituent_returns = benchmarks.get_altsoft_peer_constituent_returns(start_date=self._start_date,
                                                                                       end_date=self._end_date)

        eurekahedge_constituent_returns = benchmarks.get_eurekahedge_constituent_returns(start_date=self._start_date,
                                                                                         end_date=self._end_date)

        exposure_latest = inv_group.get_latest_exposure(as_of_date=self._as_of_date)

        start_3y = self._end_date - relativedelta(years=3)
        exposure_3y = inv_group.get_average_exposure(start_date=start_3y, end_date=self._end_date)

        start_5y = self._end_date - relativedelta(years=5)
        exposure_5y = inv_group.get_average_exposure(start_date=start_5y, end_date=self._end_date)

        start_10y = self._end_date - relativedelta(years=10)
        exposure_10y = inv_group.get_average_exposure(start_date=start_10y, end_date=self._end_date)

        factors = Factor(tickers=['SBMMTB1 Index', 'SPXT Index'])
        market_factor_returns = factors.get_returns(start_date=self._start_date,
                                                    end_date=self._end_date,
                                                    fill_na=True)

        rba = inv_group.get_rba_return_decomposition_by_date(start_date=self._start_date,
                                                             end_date=self._end_date,
                                                             factor_filter=['SYSTEMATIC',
                                                                            'REGION',
                                                                            'INDUSTRY',
                                                                            'LS_EQUITY',
                                                                            'LS_CREDIT',
                                                                            'MACRO',
                                                                            'NON_FACTOR_SECURITY_SELECTION',
                                                                            'NON_FACTOR_OUTLIER_EFFECTS'],
                                                             frequency="M")

        start_1y = self._end_date - relativedelta(years=1)
        rba_risk_1y = inv_group.get_average_risk_decomp_by_group(start_date=start_1y,
                                                                 end_date=self._end_date,
                                                                 group_type='FactorGroup1',
                                                                 frequency='M',
                                                                 wide=False)
        rba_risk_1y.rename(columns={'AvgRiskContrib': '1Y'}, inplace=True)

        rba_risk_3y = inv_group.get_average_risk_decomp_by_group(start_date=start_3y,
                                                                 end_date=self._end_date,
                                                                 group_type='FactorGroup1',
                                                                 frequency='M',
                                                                 wide=False)
        rba_risk_3y.rename(columns={'AvgRiskContrib': '3Y'}, inplace=True)

        rba_risk_5y = inv_group.get_average_risk_decomp_by_group(start_date=start_5y,
                                                                 end_date=self._end_date,
                                                                 group_type='FactorGroup1',
                                                                 frequency='M',
                                                                 wide=False)
        rba_risk_5y.rename(columns={'AvgRiskContrib': '5Y'}, inplace=True)

        rba_risk_decomp = rba_risk_1y.merge(rba_risk_3y, how='outer').merge(rba_risk_5y, how='outer')
        rba_risk_decomp = rba_risk_decomp.fillna(0)

        rba_r2_1y = inv_group.get_average_adj_r2(start_date=start_1y,
                                                 end_date=self._end_date,
                                                 frequency='M')
        rba_r2_1y.rename(columns={'AvgAdjR2': '1Y'}, inplace=True)

        rba_r2_3y = inv_group.get_average_adj_r2(start_date=start_3y,
                                                 end_date=self._end_date,
                                                 frequency='M')
        rba_r2_3y.rename(columns={'AvgAdjR2': '3Y'}, inplace=True)

        rba_r2_5y = inv_group.get_average_adj_r2(start_date=start_5y,
                                                 end_date=self._end_date,
                                                 frequency='M')
        rba_r2_5y.rename(columns={'AvgAdjR2': '5Y'}, inplace=True)

        rba_adj_r_squared = rba_r2_1y.merge(rba_r2_3y, how='outer').merge(rba_r2_5y, how='outer')

        pba_publics = inv_group.get_pba_ts_by_group(start_date=self._start_date,
                                                    end_date=self._end_date,
                                                    group_type='FactorGroup',
                                                    frequency='M',
                                                    public_or_private='Public')

        pba_privates = inv_group.get_pba_ts_by_group(start_date=self._start_date,
                                                     end_date=self._end_date,
                                                     group_type='FactorGroup',
                                                     frequency='M',
                                                     public_or_private='Private')

        report_inputs = dict()
        report_inputs['fund_inputs'] = dict()
        report_inputs['peer_inputs'] = dict()
        report_inputs['eurekahedge_inputs'] = dict()
        report_inputs['market_factor_returns'] = dict()
        for fund_id in fund_dimn['InvestmentGroupId']:
            fund_inputs = dict()
            dimn = fund_dimn[fund_dimn['InvestmentGroupId'] == fund_id]
            fund_inputs['fund_dimn'] = dimn.to_json(orient='index')
            name = dimn['InvestmentGroupName'].squeeze()

            returns = fund_monthly_returns.loc[:, fund_monthly_returns.columns.isin([name])]
            fund_inputs['fund_returns'] = returns.to_json(orient='index')

            abs_returns = abs_bmrk_returns.loc[:, abs_bmrk_returns.columns.isin([fund_id])]
            fund_inputs['abs_bmrk_returns'] = abs_returns.to_json(orient='index')

            exp_latest = exposure_latest[exposure_latest['InvestmentGroupId'] == fund_id]
            fund_inputs['exposure_latest'] = exp_latest.to_json(orient='index')

            exp_3y = exposure_3y[exposure_3y['InvestmentGroupId'] == fund_id]
            fund_inputs['exposure_3y'] = exp_3y.to_json(orient='index')

            exp_5y = exposure_5y[exposure_5y['InvestmentGroupId'] == fund_id]
            fund_inputs['exposure_5y'] = exp_5y.to_json(orient='index')

            exp_10y = exposure_10y[exposure_10y['InvestmentGroupId'] == fund_id]
            fund_inputs['exposure_10y'] = exp_10y.to_json(orient='index')

            fund_rba = rba.iloc[:, rba.columns.get_level_values(1) == fund_id]
            fund_rba.columns = fund_rba.columns.droplevel(0).droplevel(0)
            fund_inputs['rba'] = fund_rba.to_json(orient='index')

            decomp = rba_risk_decomp[rba_risk_decomp['InvestmentGroupId'] == fund_id]
            fund_inputs['rba_risk_decomp'] = decomp.to_json(orient='index')

            r2 = rba_adj_r_squared[rba_adj_r_squared['InvestmentGroupId'] == fund_id]
            fund_inputs['rba_adj_r_squared'] = r2.to_json(orient='index')

            publics_index = pba_publics.columns.get_level_values(1) == fund_id
            fund_inputs['pba_publics'] = pba_publics.iloc[:, publics_index].to_json(orient='index')

            privates_index = pba_privates.columns.get_level_values(1) == fund_id
            fund_inputs['pba_privates'] = pba_privates.iloc[:, privates_index].to_json(orient='index')

            report_inputs['fund_inputs'][name] = fund_inputs

        for peer in gcm_peer_returns.columns:
            peer_inputs = dict()
            peer_inputs['gcm_peer_returns'] = gcm_peer_returns[peer].to_json(orient='index')
            peer_index = gcm_peer_constituent_returns.columns.get_level_values(0) == peer
            constituents = gcm_peer_constituent_returns.iloc[:, peer_index]
            peer_inputs['gcm_peer_constituent_returns'] = constituents.to_json(orient='index')

            report_inputs['peer_inputs'][peer] = peer_inputs

        for eh_name in eurekahedge_returns.columns:
            eh_inputs = dict()
            eh_inputs['eurekahedge_returns'] = eurekahedge_returns[eh_name].to_json(orient='index')
            eh_index = eurekahedge_constituent_returns.columns.get_level_values(0) == eh_name
            eh_constituents = eurekahedge_constituent_returns.iloc[:, eh_index]
            eh_inputs['eurekahedge_constituent_returns'] = eh_constituents.to_json(orient='index')

            report_inputs['eurekahedge_inputs'][eh_name] = eh_inputs

        report_inputs['market_factor_returns'] = market_factor_returns.to_json(orient='index')

        peers = pd.concat([fund_dimn['ReportingPeerGroup'], fund_dimn['StrategyPeerGroup']]).tolist()
        report_inputs['filtered_peers'] = peers

        return report_inputs

    def generate_inputs_and_write_to_datalake(self) -> dict:
        inputs = self.get_performance_quality_report_inputs()
        asofdate = self._as_of_date.strftime('%Y-%m-%d')

        fund_names = sorted(list(inputs['fund_inputs'].keys()))
        for fund in fund_names:
            fund_input = json.dumps(inputs['fund_inputs'][fund])
            write_params = AzureDataLakeDao.create_get_data_params(
                self._underlying_data_location,
                fund.replace('/', '') + '_fund_inputs_' + asofdate + '.json',
                retry=False,
            )
            self._runner.execute(
                params=write_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, fund_input)
            )

        peer_names = list(set(inputs['filtered_peers']))
        peer_names = [x for x in peer_names if pd.isnull(x) is False]
        peer_names = sorted(peer_names)
        for peer in peer_names:
            peer_input = json.dumps(inputs['peer_inputs'][peer])
            write_params = AzureDataLakeDao.create_get_data_params(
                self._underlying_data_location,
                peer.replace('/', '') + '_peer_inputs_' + asofdate + '.json',
                retry=False,
            )
            self._runner.execute(
                params=write_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, peer_input)
            )

        eh_names = sorted(list(inputs['eurekahedge_inputs'].keys()))
        for eh in eh_names:
            eh_input = json.dumps(inputs['eurekahedge_inputs'][eh])
            write_params = AzureDataLakeDao.create_get_data_params(
                self._underlying_data_location,
                eh.replace('/', '') + '_eurekahedge_inputs_' + asofdate + '.json',
                retry=False,
            )
            self._runner.execute(
                params=write_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, eh_input)
            )

        market_factor_returns = json.dumps(inputs['market_factor_returns'])
        write_params = AzureDataLakeDao.create_get_data_params(
            self._underlying_data_location,
            'market_factor_returns_' + asofdate + '.json',
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, market_factor_returns)
        )

        funds = dict({'fund_names': fund_names, 'peer_groups': peer_names})
        funds = json.dumps(funds)
        return funds

    def run(self, **kwargs):
        return self.generate_inputs_and_write_to_datalake()
