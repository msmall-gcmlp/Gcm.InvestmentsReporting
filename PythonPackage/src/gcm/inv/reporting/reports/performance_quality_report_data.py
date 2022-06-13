import pandas as pd
import json

from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.dataprovider.attribution import Attribution
from gcm.inv.dataprovider.inv_dwh.attribution_query import AttributionQuery
from gcm.inv.dataprovider.inv_dwh.benchmarking_query import BenchmarkingQuery
from gcm.inv.dataprovider.inv_dwh.factors_query import FactorsQuery
from gcm.inv.dataprovider.inv_dwh.external_inv_dimensions_query import ExternalInvDimensionsQuery
from gcm.inv.dataprovider.pub_dwh.pub_inv_dimensions_query import PubInvDimensionsQuery
from gcm.inv.dataprovider.inv_dwh.external_inv_returns_query import ExternalInvReturnsQuery
from gcm.inv.dataprovider.pub_dwh.pub_inv_returns_query import PubInvReturnsQuery
from gcm.inv.dataprovider.entity_master import EntityMaster
from gcm.inv.dataprovider.benchmarking import Benchmarking
from gcm.inv.dataprovider.factors import Factors
from gcm.inv.dataprovider.investments import Investments
from gcm.inv.dataprovider.investment_returns import InvestmentReturns
from gcm.inv.dataprovider.investment_exposure import InvestmentExposure
from gcm.inv.dataprovider.pub_dwh.pub_inv_exposure_query import PubInvExposureQuery
from pandas._libs.tslibs.offsets import relativedelta
from gcm.inv.dataprovider.entity_master_enum import SourceDimension

from .reporting_runner_base import ReportingRunnerBase
from gcm.inv.quantlib.timeseries.analytics import Analytics


class PerformanceQualityReportData(ReportingRunnerBase):

    def __init__(self, runner, start_date, end_date, as_of_date, investment_ids):
        super().__init__(runner=runner)
        self._start_date = start_date
        self._end_date = end_date
        self._as_of_date = as_of_date
        self._analytics = Analytics()

        external_inv_returns_query = ExternalInvReturnsQuery(runner=runner, as_of_date=as_of_date)
        benchmarking_query = BenchmarkingQuery(runner=runner, as_of_date=as_of_date)
        pub_inv_dimensions_query = PubInvDimensionsQuery(runner=runner, as_of_date=as_of_date)
        external_inv_dimensions_query = ExternalInvDimensionsQuery(runner=runner, as_of_date=as_of_date)
        entity_master = EntityMaster(runner=runner, as_of_date=as_of_date)
        attribution_query = AttributionQuery(runner=runner, as_of_date=as_of_date)
        factors_query = FactorsQuery(runner=runner, as_of_date=as_of_date)
        pub_inv_exposure_query = PubInvExposureQuery(runner=runner, as_of_date=as_of_date)

        pub_inv_returns_query = PubInvReturnsQuery(runner=runner, as_of_date=as_of_date)

        benchmarking = Benchmarking(benchmarking_query=benchmarking_query,
                                    external_inv_returns_query=external_inv_returns_query)

        investments = Investments(pub_inv_dimensions_query=pub_inv_dimensions_query,
                                  external_inv_dimensions_query=external_inv_dimensions_query,
                                  entity_master=entity_master,
                                  benchmarking=benchmarking)

        attribution = Attribution(attribution_query=attribution_query, entity_master=entity_master)

        factors = Factors(factors_query=factors_query)

        investment_returns = InvestmentReturns(investment_returns_query=external_inv_returns_query,
                                               entity_master=entity_master,
                                               pub_inv_returns_query=pub_inv_returns_query)

        investment_exposure = InvestmentExposure(entity_master=entity_master,
                                                 pub_inv_exposure_query=pub_inv_exposure_query)

        self._investments = investments
        self._attribution = attribution
        self._benchmarking = benchmarking
        self._factors = factors
        self._inv_returns = investment_returns
        self._inv_exposure = investment_exposure
        self._entity_master = entity_master

        self._entity_type = 'ARS PFUND'
        self._status = 'EMM'
        self._investment_ids = investment_ids

    def get_performance_quality_report_inputs(self):
        # pre-filtering to EMMs to avoid performance issues. refactor later to occur behind the scenes in data provider
        if self._investment_ids is None:
            pub_ids = self._investments.get_filtered_pub_investment_ids(statuses='EMM')
            pub_ids = [str(id) for id in pub_ids]
            pub_id = SourceDimension.Pub_InvestmentDimn.value
            investment_ids = self._entity_master.get_investment_ids_by_source_id(source_inv_ids=pub_ids,
                                                                                 source_id=pub_id)
            investment_ids = investment_ids['InvestmentId'].tolist()

            investment_master = self._entity_master.get_investment_reporting_managers()
            master_subset = investment_master[investment_master['InvestmentId'].isin(investment_ids)]
            subset_groups = master_subset['InvestmentGroupId'].unique().tolist()
            subset_investments = investment_master[investment_master['InvestmentGroupId'].isin(subset_groups)]
            investment_ids = subset_investments['InvestmentId'].unique().tolist()

            include_filters = dict(status=[self._status])
            exclude_filters = dict(strategy=['Other', 'Aggregated Prior Period Adjustment'])
            exclude_gcm_portfolios = True
        else:
            investment_ids = self._investment_ids.copy()
            include_filters = None
            exclude_filters = None
            exclude_gcm_portfolios = False

        fund_dimn = self._investments.get_condensed_investment_group_dimensions(as_dataframe=True,
                                                                                investment_ids=investment_ids)

        filtered_dimn = \
            self._investments.get_filtered_investment_group_dimensions(include_filters=include_filters,
                                                                       exclude_filters=exclude_filters,
                                                                       exclude_gcm_portfolios=exclude_gcm_portfolios,
                                                                       investment_ids=investment_ids)

        filtered_dimn = filtered_dimn[['InvestmentGroupId',
                                       'PubInvestmentGroupId',
                                       'InvestmentGroupName',
                                       'AbsoluteBenchmarkId',
                                       'AbsoluteBenchmarkName',
                                       'EurekahedgeBenchmark',
                                       'InceptionDate',
                                       'InvestmentStatus',
                                       'ReportingPeerGroup',
                                       'StrategyPeerGroup',
                                       'Strategy',
                                       'SubStrategy',
                                       'FleScl',
                                       'RiskModelExpectedReturn',
                                       'RiskModelExpectedVol']]
        returns_source = [SourceDimension.Pub_InvestmentDimn]
        fund_monthly_returns = \
            self._inv_returns.get_investment_group_monthly_returns(start_date=self._start_date,
                                                                   end_date=self._end_date,
                                                                   as_dataframe=True,
                                                                   investment_ids=investment_ids,
                                                                   priority_waterfall=returns_source)

        abs_bmrk_returns = \
            self._benchmarking.get_absolute_benchmark_returns(investment_group_ids=fund_dimn['InvestmentGroupId'],
                                                              start_date=self._start_date,
                                                              end_date=self._end_date)

        eurekahedge_returns = self._benchmarking.get_eurekahedge_benchmark_returns(start_date=self._start_date,
                                                                                   end_date=self._end_date)

        gcm_peer_returns = self._benchmarking.get_altsoft_peer_benchmark_returns(start_date=self._start_date,
                                                                                 end_date=self._end_date)

        gcm_peer_constituent_returns = \
            self._benchmarking.get_altsoft_peer_constituent_returns(start_date=self._start_date,
                                                                    end_date=self._end_date)

        eurekahedge_constituent_returns = \
            self._benchmarking.get_eurekahedge_benchmark_constituent_returns(start_date=self._start_date,
                                                                             end_date=self._end_date)

        exposure_latest = self._inv_exposure.get_latest_investment_group_exposure(investment_ids=investment_ids,
                                                                                  as_of_date=self._end_date)

        start_3y = self._end_date - relativedelta(years=3)
        exposure_3y = self._inv_exposure.get_average_investment_group_exposure(investment_ids=investment_ids,
                                                                               start_date=start_3y,
                                                                               end_date=self._end_date)

        start_5y = self._end_date - relativedelta(years=5)
        exposure_5y = self._inv_exposure.get_average_investment_group_exposure(investment_ids=investment_ids,
                                                                               start_date=start_5y,
                                                                               end_date=self._end_date)

        start_10y = self._end_date - relativedelta(years=10)
        exposure_10y = self._inv_exposure.get_average_investment_group_exposure(investment_ids=investment_ids,
                                                                                start_date=start_10y,
                                                                                end_date=self._end_date)

        market_factor_returns = self._factors.get_factor_returns(tickers=['SBMMTB1 Index', 'SPXT Index'],
                                                                 start_date=self._start_date,
                                                                 end_date=self._end_date,
                                                                 fill_na=True)

        rba = self._attribution.get_rba_ts_by_group(investment_ids=investment_ids,
                                                    start_date=self._start_date,
                                                    end_date=self._end_date,
                                                    group_type='FactorGroup1',
                                                    frequency='M')

        start_1y = self._end_date - relativedelta(years=1)
        rba_risk_1y = self._attribution.get_average_risk_decomp_by_group(investment_ids=investment_ids,
                                                                         start_date=start_1y,
                                                                         end_date=self._end_date,
                                                                         group_type='FactorGroup1',
                                                                         frequency='M',
                                                                         wide=False)
        rba_risk_1y.rename(columns={'AvgRiskContrib': '1Y'}, inplace=True)

        rba_risk_3y = self._attribution.get_average_risk_decomp_by_group(investment_ids=investment_ids,
                                                                         start_date=start_3y,
                                                                         end_date=self._end_date,
                                                                         group_type='FactorGroup1',
                                                                         frequency='M',
                                                                         wide=False)
        rba_risk_3y.rename(columns={'AvgRiskContrib': '3Y'}, inplace=True)

        rba_risk_5y = self._attribution.get_average_risk_decomp_by_group(investment_ids=investment_ids,
                                                                         start_date=start_5y,
                                                                         end_date=self._end_date,
                                                                         group_type='FactorGroup1',
                                                                         frequency='M',
                                                                         wide=False)
        rba_risk_5y.rename(columns={'AvgRiskContrib': '5Y'}, inplace=True)

        rba_risk_decomp = rba_risk_1y.merge(rba_risk_3y, how='outer').merge(rba_risk_5y, how='outer')
        rba_risk_decomp = rba_risk_decomp.fillna(0)

        rba_r2_1y = self._attribution.get_average_adj_r2(investment_ids=investment_ids,
                                                         start_date=start_1y,
                                                         end_date=self._end_date,
                                                         frequency='M')
        rba_r2_1y.rename(columns={'AvgAdjR2': '1Y'}, inplace=True)

        rba_r2_3y = self._attribution.get_average_adj_r2(investment_ids=investment_ids,
                                                         start_date=start_3y,
                                                         end_date=self._end_date,
                                                         frequency='M')
        rba_r2_3y.rename(columns={'AvgAdjR2': '3Y'}, inplace=True)

        rba_r2_5y = self._attribution.get_average_adj_r2(investment_ids=investment_ids,
                                                         start_date=start_5y,
                                                         end_date=self._end_date,
                                                         frequency='M')
        rba_r2_5y.rename(columns={'AvgAdjR2': '5Y'}, inplace=True)

        rba_adj_r_squared = rba_r2_1y.merge(rba_r2_3y, how='outer').merge(rba_r2_5y, how='outer')

        pba_publics = self._attribution.get_pba_ts_by_group(investment_ids=investment_ids,
                                                            start_date=self._start_date,
                                                            end_date=self._end_date,
                                                            group_type='FactorGroup',
                                                            frequency='M',
                                                            public_or_private='Public')

        pba_privates = self._attribution.get_pba_ts_by_group(investment_ids=investment_ids,
                                                             start_date=self._start_date,
                                                             end_date=self._end_date,
                                                             group_type='FactorGroup',
                                                             frequency='M',
                                                             public_or_private='Private')

        report_inputs = dict()
        report_inputs['fund_dimn'] = filtered_dimn.to_json(orient='index')
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
