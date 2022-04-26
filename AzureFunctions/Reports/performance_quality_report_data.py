import datetime as dt
import pandas as pd
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
        self._investments = Investments(
            pub_inv_dimensions_query=PubInvDimensionsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1)),
            external_inv_dimensions_query=ExternalInvDimensionsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1)),
            entity_master=EntityMaster(runner=runner, as_of_date=dt.date(2020, 10, 1)),
            benchmarking=Benchmarking(benchmarking_query=BenchmarkingQuery(runner=runner,
                                                                           as_of_date=dt.date(2020, 10, 1))))
        self._attribution = Attribution(attribution_query=AttributionQuery(runner=runner, as_of_date=as_of_date),
                                        investments=self._investments)

        self._benchmarking = Benchmarking(BenchmarkingQuery(runner=runner, as_of_date=as_of_date))
        self._factors = Factors(FactorsQuery(runner=runner, as_of_date=as_of_date))
        self._inv_returns = \
            InvestmentReturns(investment_returns_query=ExternalInvReturnsQuery(runner=runner,
                                                                               as_of_date=dt.date(2020, 10, 1)),
                              entity_master=EntityMaster(runner=runner, as_of_date=dt.date(2020, 10, 1)),
                              pub_inv_returns_query=PubInvReturnsQuery(runner=runner,
                                                                       as_of_date=dt.date(2020, 10, 1)))
        self._entity_type = 'ARS PFUND'
        self._group = params['group']

    def get_performance_quality_report_inputs(self):
        #investment_ids=[34411, 41096, 139998]
        fund_dimn = self._investments.get_condensed_investment_group_dimensions(as_dataframe=True)
        fund_monthly_returns = \
            self._inv_returns.get_investment_group_monthly_returns(start_date=self._start_date,
                                                                   end_date=self._end_date,
                                                                   as_dataframe=True,
                                                                   investment_group_ids=fund_dimn['InvestmentGroupId'])
        
        include_filters = dict(status=['EMM'])
        exclude_filters = dict(strategy=['Other', 'Aggregated Prior Period Adjustment'])
        emms = self._investments.get_filtered_investment_group_dimensions(include_filters=include_filters,
                                                                          exclude_filters=exclude_filters,
                                                                          exclude_gcm_portfolios=True,
                                                                          dimensions=fund_dimn)
        fund_dimn = fund_dimn.to_json(orient='index')
        fund_monthly_returns = fund_monthly_returns.to_json(orient='index')
        emms = emms.to_json(orient='index')
        return fund_dimn, fund_monthly_returns, emms

    def run(self, **kwargs):
        return self.get_performance_quality_report_inputs()
