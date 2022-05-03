import datetime as dt
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
from .reporting_runner_base import ReportingRunnerBase


class PerformanceQualityReportData(ReportingRunnerBase):

    def __init__(self, runner, start_date, end_date, as_of_date, params):
        super().__init__(runner=runner)
        self._start_date = start_date
        self._end_date = end_date
        self._as_of_date = as_of_date
        external_inv_returns_query = ExternalInvReturnsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1))
        benchmarking = Benchmarking(benchmarking_query=BenchmarkingQuery(runner=runner,
                                                                         as_of_date=dt.date(2020, 10, 1)),
                                    external_inv_returns_query=external_inv_returns_query)
        self._investments = Investments(
            pub_inv_dimensions_query=PubInvDimensionsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1)),
            external_inv_dimensions_query=ExternalInvDimensionsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1)),
            entity_master=EntityMaster(runner=runner, as_of_date=dt.date(2020, 10, 1)),
            benchmarking=benchmarking)
        self._attribution = Attribution(attribution_query=AttributionQuery(runner=runner, as_of_date=as_of_date),
                                        investments=self._investments)

        self._benchmarking = benchmarking
        self._factors = Factors(FactorsQuery(runner=runner, as_of_date=as_of_date))
        self._inv_returns = \
            InvestmentReturns(investment_returns_query=external_inv_returns_query,
                              entity_master=EntityMaster(runner=runner, as_of_date=dt.date(2020, 10, 1)),
                              pub_inv_returns_query=PubInvReturnsQuery(runner=runner,
                                                                       as_of_date=dt.date(2020, 10, 1)))
        self._entity_type = params['vertical'] + ' ' + params['entity']
        self._filter = params['filter']

    def get_performance_quality_report_inputs(self):
        investment_ids = [34411, 41096, 139998]
        #investment_ids = None
        fund_dimn = self._investments.get_condensed_investment_group_dimensions(as_dataframe=True,
                                                                                investment_ids=investment_ids)

        include_filters = dict(status=[self._filter])
        exclude_filters = dict(strategy=['Other', 'Aggregated Prior Period Adjustment'])
        filtered_dimn = self._investments.get_filtered_investment_group_dimensions(include_filters=include_filters,
                                                                                   exclude_filters=exclude_filters,
                                                                                   exclude_gcm_portfolios=True,
                                                                                   investment_ids=investment_ids)

        filtered_dimn = filtered_dimn[['InvestmentGroupId',
                                      'InvestmentGroupName',
                                      'AbsoluteBenchmarkId',
                                      'AbsoluteBenchmarkName',
                                      'EurekahedgeBenchmark',
                                      'InceptionDate',
                                      'InvestmentStatus',
                                      'ReportingPeerGroup',
                                      'Strategy',
                                      'SubStrategy']]

        fund_monthly_returns = \
            self._inv_returns.get_investment_group_monthly_returns(start_date=self._start_date,
                                                                   end_date=self._end_date,
                                                                   as_dataframe=True,
                                                                   investment_ids=investment_ids)

        abs_bmrk_returns = \
            self._benchmarking.get_absolute_benchmark_returns(investment_group_ids=fund_dimn['InvestmentGroupId'],
                                                              start_date=self._start_date,
                                                              end_date=self._end_date)

        eurekahedge_returns = self._benchmarking.get_eurekahedge_benchmark_returns(start_date=self._start_date,
                                                                                   end_date=self._end_date)

        gcm_peer_returns = self._benchmarking.get_altsoft_peer_benchmark_returns(start_date=self._start_date,
                                                                                 end_date=self._end_date)

        report_inputs = {}
        report_inputs['fund_dimn'] = filtered_dimn.to_json(orient='index')
        report_inputs['fund_returns'] = fund_monthly_returns.to_json(orient='index')
        report_inputs['eurekahedge_returns'] = eurekahedge_returns.to_json(orient='index')
        report_inputs['abs_bmrk_returns'] = abs_bmrk_returns.to_json(orient='index')
        report_inputs['gcm_peer_returns'] = gcm_peer_returns.to_json(orient='index')

        return report_inputs

    def run(self, **kwargs):
        return self.get_performance_quality_report_inputs()
