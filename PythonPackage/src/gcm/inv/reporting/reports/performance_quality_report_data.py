import pandas as pd
import json
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

        fund_dimn = fund_dimn[['InvestmentGroupId', 'PubInvestmentGroupId', 'InvestmentGroupName',
                               'AbsoluteBenchmarkId', 'AbsoluteBenchmarkName', 'EurekahedgeBenchmark',
                               'InceptionDate', 'InvestmentStatus', 'ReportingPeerGroup', 'StrategyPeerGroup',
                               'Strategy', 'SubStrategy', 'FleScl', 'RiskModelExpectedReturn', 'RiskModelExpectedVol']]

        # returns_source = [SourceDimension.Pub_InvestmentDimn]
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

        rba = inv_group.get_rba_ts_by_factor_group(start_date=self._start_date,
                                                   end_date=self._end_date,
                                                   group_type='FactorGroup1',
                                                   frequency='M')

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
        report_inputs['fund_dimn'] = fund_dimn.to_json(orient='index')
        report_inputs['fund_returns'] = fund_monthly_returns.to_json(orient='index')
        report_inputs['eurekahedge_returns'] = eurekahedge_returns.to_json(orient='index')
        report_inputs['abs_bmrk_returns'] = abs_bmrk_returns.to_json(orient='index')
        report_inputs['gcm_peer_returns'] = gcm_peer_returns.to_json(orient='index')
        report_inputs['gcm_peer_constituent_returns'] = gcm_peer_constituent_returns.to_json(orient='index')
        report_inputs['eurekahedge_constituent_returns'] = eurekahedge_constituent_returns.to_json(orient='index')
        report_inputs['exposure_latest'] = exposure_latest.to_json(orient='index')
        report_inputs['exposure_3y'] = exposure_3y.to_json(orient='index')
        report_inputs['exposure_5y'] = exposure_5y.to_json(orient='index')
        report_inputs['exposure_10y'] = exposure_10y.to_json(orient='index')
        report_inputs['market_factor_returns'] = market_factor_returns.to_json(orient='index')
        report_inputs['rba'] = rba.to_json(orient='index')
        report_inputs['rba_risk_decomp'] = rba_risk_decomp.to_json(orient='index')
        report_inputs['rba_adj_r_squared'] = rba_adj_r_squared.to_json(orient='index')
        report_inputs['pba_publics'] = pba_publics.to_json(orient='index')
        report_inputs['pba_privates'] = pba_privates.to_json(orient='index')

        return report_inputs

    def generate_inputs_and_write_to_datalake(self) -> dict:
        inputs = self.get_performance_quality_report_inputs()
        data_to_write = json.dumps(inputs)
        write_location = "lab/rqs/azurefunctiondata"
        write_params = AzureDataLakeDao.create_get_data_params(
            write_location,
            "performance_quality_report_inputs.json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write)
        )
        fund_dimn = pd.read_json(inputs['fund_dimn'], orient='index')
        fund_names = fund_dimn['InvestmentGroupName'].tolist()
        fund_names = sorted(fund_names)

        peer_groups = fund_dimn['ReportingPeerGroup'].unique().tolist()
        # remove nas
        peer_groups = [i for i in peer_groups if i]
        peer_groups = sorted(peer_groups)

        funds = dict({'fund_names': fund_names, 'peer_groups': peer_groups})
        funds = json.dumps(funds)
        return funds

    def run(self, **kwargs):
        return self.generate_inputs_and_write_to_datalake()
