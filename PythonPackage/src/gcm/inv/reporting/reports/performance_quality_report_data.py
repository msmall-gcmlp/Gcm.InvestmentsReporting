import datetime as dt
import ast
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
from .reporting_runner_base import ReportingRunnerBase
from gcm.inv.quantlib.timeseries.analytics import Analytics


class PerformanceQualityReportData(ReportingRunnerBase):

    def __init__(self, runner, start_date, end_date, as_of_date, params):
        super().__init__(runner=runner)
        self._start_date = start_date
        self._end_date = end_date
        self._as_of_date = as_of_date
        self._analytics = Analytics()

        external_inv_returns_query = ExternalInvReturnsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1))
        benchmarking_query = BenchmarkingQuery(runner=runner, as_of_date=dt.date(2020, 10, 1))
        pub_inv_dimensions_query = PubInvDimensionsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1))
        external_inv_dimensions_query = ExternalInvDimensionsQuery(runner=runner, as_of_date=dt.date(2020, 10, 1))
        entity_master = EntityMaster(runner=runner, as_of_date=dt.date(2020, 10, 1))
        attribution_query = AttributionQuery(runner=runner, as_of_date=as_of_date)
        factors_query = FactorsQuery(runner=runner, as_of_date=as_of_date)

        pub_inv_returns_query = PubInvReturnsQuery(runner=runner,
                                                   as_of_date=dt.date(2020, 10, 1))

        benchmarking = Benchmarking(benchmarking_query=benchmarking_query,
                                    external_inv_returns_query=external_inv_returns_query)

        investments = Investments(pub_inv_dimensions_query=pub_inv_dimensions_query,
                                  external_inv_dimensions_query=external_inv_dimensions_query,
                                  entity_master=entity_master,
                                  benchmarking=benchmarking)

        attribution = Attribution(attribution_query=attribution_query, investments=investments)

        factors = Factors(factors_query=factors_query)

        investment_returns = InvestmentReturns(investment_returns_query=external_inv_returns_query,
                                               entity_master=entity_master,
                                               pub_inv_returns_query=pub_inv_returns_query)
        self._investments = investments
        self._attribution = attribution
        self._benchmarking = benchmarking
        self._factors = factors
        self._inv_returns = investment_returns

        self._entity_type = params.get('vertical') + ' ' + params.get('entity')
        self._status = params.get('status')
        self._params = params

    @property
    def _investment_ids(self):
        if self._params.get('investment_ids') is None:
            return None
        else:
            return ast.literal_eval(self._params.get('investment_ids'))

    def get_performance_quality_report_inputs(self):
        investment_ids = self._investment_ids.copy() if self._investment_ids is not None else None
        fund_dimn = self._investments.get_condensed_investment_group_dimensions(as_dataframe=True,
                                                                                investment_ids=investment_ids)

        include_filters = dict(status=[self._status])
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

        gcm_peer_constituent_returns = \
            self._benchmarking.get_altsoft_peer_constituent_returns(start_date=self._start_date,
                                                                    end_date=self._end_date)

        eurekahedge_constituent_returns = \
            self._benchmarking.get_eurekahedge_benchmark_constituent_returns(start_date=self._start_date,
                                                                             end_date=self._end_date)

        report_inputs = dict()
        report_inputs['fund_dimn'] = filtered_dimn.to_json(orient='index')
        report_inputs['fund_returns'] = fund_monthly_returns.to_json(orient='index')
        report_inputs['eurekahedge_returns'] = eurekahedge_returns.to_json(orient='index')
        report_inputs['abs_bmrk_returns'] = abs_bmrk_returns.to_json(orient='index')
        report_inputs['gcm_peer_returns'] = gcm_peer_returns.to_json(orient='index')
        report_inputs['gcm_peer_constituent_returns'] = gcm_peer_constituent_returns.to_json(orient='index')
        report_inputs['eurekahedge_constituent_returns'] = eurekahedge_constituent_returns.to_json(orient='index')

        return report_inputs

    def download_performance_quality_report_inputs(self) -> dict:
        location = "lab/rqs/azurefunctiondata"
        read_params = AzureDataLakeDao.create_get_data_params(
            location,
            "performance_quality_report_inputs.json",
            retry=False,
        )
        file = self._runner.execute(
            params=read_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(read_params)
        )
        return json.loads(file.content)

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
        fund_names = pd.read_json(inputs['fund_dimn'], orient='index')['InvestmentGroupName'].tolist()
        return fund_names

    def run(self, **kwargs):
        return self.generate_inputs_and_write_to_datalake()
